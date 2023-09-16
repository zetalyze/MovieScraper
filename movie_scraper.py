import concurrent.futures
import logging
import re
import time

import pandas as pd
import requests

logger = logging.getLogger("MOVIE_SCRAPER")

logging.basicConfig(level=logging.INFO)

ROOT_LINK = "https://www.the-numbers.com"
HEADERS = {"User-agent": "Chrome/47.0.2526.80"}

YEAR = 2022
ROOT_RESPONSE = requests.get(rf"{ROOT_LINK}/market/{YEAR}/top-grossing-movies", headers=HEADERS)
ROOT_RESPONSE.raise_for_status()

ROOT_HTML = ROOT_RESPONSE.text

MAX_THREADING_WORKERS = 100


def get_summary_endpoint(movie: str) -> str:
    match = re.search(f'<a href="(?P<endpoint>.*#tab=summary)">{re.escape(movie.removesuffix("…"))}', ROOT_HTML)
    if match is None:
        raise ValueError(f"Got no summary response for movie: {movie!r}")
    return match["endpoint"]


def get_budget(movie: str, summary_response: str) -> str:
    match = re.search(f"Production&nbsp;Budget:.*(?P<budget>\$[0-9|,]+)", summary_response)
    if match is None:
        logger.info(f"Got no budget for movie {movie!r}")
        return "N/A"
    return match["budget"]


def get_rating(movie: str, summary_response: str) -> str:
    match = re.search(f"MPAA&nbsp;Rating:.*\n.+<a.+>(?P<rating>.*)</a>", summary_response)
    if match is None:
        logger.info(f"Got no rating for movie {movie!r}")
        return "N/A"
    return match["rating"]


def get_box_office_stats(movie: str, rank: int, box_office_response: str) -> pd.DataFrame:
    html_response = pd.read_html(box_office_response)
    watned_columns = ["Date", "Rank", "Gross", "%YD", "%LW", "Theaters", "Per Theater", "Total\xa0Gross", "Days"]
    for data in html_response:
        if len(data.columns) == len(watned_columns):
            if data.columns.to_list() == watned_columns:
                return pd.DataFrame(data)[["Date", "Gross", "Theaters", "Days"]].fillna(0.0).assign(Rank=rank)
    logger.info(f"Got no Box Office data for movie: {movie!r}")
    return pd.DataFrame({"Date": "N/A", "Gross": "N/A", "Theaters": "N/A", "Days": "N/A", "Rank": rank}, index=[0])


def get_budget_rating_box_office(movie: str, rank: int) -> dict[str, dict[str, str] | pd.DataFrame]:
    logger.info(f"Gathering data for movie: {movie!r}")
    summary_endpoint = get_summary_endpoint(movie)
    summary_response = requests.get(f"{ROOT_LINK}/{summary_endpoint}", headers=HEADERS).text

    box_office_endpoint = summary_endpoint.replace("#tab=summary", "#tab=box-office")
    box_office_reponse = requests.get(f"{ROOT_LINK}/{box_office_endpoint}", headers=HEADERS).text
    result = {
        "budget": {rank: get_budget(movie=movie, summary_response=summary_response)},
        "rating": {rank: get_rating(movie=movie, summary_response=summary_response)},
        "box_office": get_box_office_stats(movie=movie, rank=rank, box_office_response=box_office_reponse),
    }
    logger.info(f"Finished gathering data for movie: {movie!r}")
    return result


def main():
    start_time = time.time()
    logger.info(f"Starting script for {YEAR=} using {MAX_THREADING_WORKERS=}")

    # Last two rows are for totals
    movies = pd.DataFrame(pd.read_html(ROOT_HTML)[0])[:-2]
    movies["Rank"] = movies["Rank"].astype(int)
    logger.info(f"Scraping movie data for {len(movies)} movies from {ROOT_LINK}")

    budgets: dict[str, str] = dict()
    ratings: dict[str, str] = dict()
    box_office: list[pd.DataFrame] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADING_WORKERS) as executor:
        for result in executor.map(get_budget_rating_box_office, movies["Movie"], movies["Rank"]):
            budgets |= result["budget"]
            ratings |= result["rating"]
            box_office.append(result["box_office"])

    budget_and_rating = (
        pd.concat([pd.Series(budgets, name="Production Budget"), pd.Series(ratings, name="MPAA Rating")], axis=1)
        .reset_index()
        .rename(columns={"index": "Rank"})
    )
    movies = movies.merge(budget_and_rating, on="Rank", validate="many_to_one")

    out = pd.concat(box_office, axis=0).merge(movies, on="Rank", validate="many_to_one")[
        ["Rank", "Movie", "Production Budget", "MPAA Rating", "Genre", "Date", "Gross", "Theaters", "Days"]
    ]
    out["date_for_sorting"] = out["Date"].map(lambda date: pd.Timestamp(date) if date != "N/A" else date)
    out["Days"] = out["Days"].map(lambda days: int(days) if days != "N/A" else days)
    out.sort_values(by=["Rank", "date_for_sorting"]).drop(columns=["date_for_sorting"]).to_csv(
        f"top_grossing_{YEAR}_movies.csv", index=False
    )

    end_time = time.time()
    logger.info(f"Script completed successfully in {round(end_time-start_time, 1)} seconds!")


if __name__ == "__main__":
    main()
