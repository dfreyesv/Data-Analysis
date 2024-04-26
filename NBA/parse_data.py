import os
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import pandas as pd
import time, sys

def parse_html(box_score):
    try:
        with open(box_score, encoding="utf-8") as f:
            html = f.read()
    except UnicodeDecodeError:
        with open(box_score, encoding="latin1") as f:
            html = f.read()
    
    soup = BeautifulSoup(html, "html.parser")
    [s.decompose() for s in soup.select("tr.over_header")]
    [s.decompose() for s in soup.select("tr.thead")]
    return soup

def read_line_score(soup):
    line_score = pd.read_html(str(soup), attrs={"id": "line_score"})[0]
    cols = list(line_score.columns)
    cols[0] = "team"
    cols[-1] = "total"
    line_score.columns = cols
    line_score = line_score[["team","total"]]
    
    return line_score

def read_stats(soup, team, stat):
    df = pd.read_html(str(soup), attrs={"id": f"box-{team}-game-{stat}"}, index_col=0)[0]
    df = df.apply(pd.to_numeric, errors="ignore")
    if stat == "basic":
        df = pd.read_html(str(soup), attrs={"id": f"box-{team}-game-{stat}"}, index_col=0)[0]
        df["+/-"] = df["+/-"].apply(lambda x: x.replace("+","") if isinstance(x, str) else x)
    else:
        df = pd.read_html(str(soup), attrs={"id": f"box-{team}-game-{stat}"}, index_col=0)[0]

    df["MP"] = df["MP"].apply(convert_to_decimal_minutes)
    df = df.apply(pd.to_numeric, errors="coerce")
    return df

def read_season_info(soup):
    nav = soup.select("#bottom_nav_container")[0]
    hrefs = [a["href"] for a in nav.find_all("a")]
    season = os.path.basename(hrefs[1]).split("_")[0]
    return season

# Function to convert "MM:SS" format to decimal minutes
def convert_to_decimal_minutes(time_str):
    if ":" in time_str:
        minutes, seconds = map(int, time_str.split(":"))
        decimal_minutes = minutes + seconds / 60
        return decimal_minutes
    else:
        return time_str
    
def get_file_stats(box_scores):
    
    games_stats = []
    players_stats = []
    
    for box_score in box_scores:

        soup = parse_html(box_score)
        line_score = read_line_score(soup)
        teams = list(line_score["team"])
        player_summaries = []
        game_summaries = []
        base_cols = None

        for team in teams:
            basic = read_stats(soup, team, "basic")
            advanced = read_stats(soup, team, "advanced")
            summary = pd.concat([basic, advanced], axis=1)
            summary.columns = summary.columns.str.lower()
            maxes = summary.loc[summary.index != "Team Totals"].max()
            maxes.index = maxes.index.str.lower() + "_max"

            game =  pd.concat([summary.loc["Team Totals"], maxes], axis=0).to_frame().T

            summary["team"] = team
            game["team"] = team

            summary["team_opp"] = line_score.loc[line_score["team"] != team]["team"].iloc[0]
            game["team_opp"] = line_score.loc[line_score["team"] != team]["team"].iloc[0]

            summary["home"] = 0 if team == teams[0] else 1
            game["home"] = 0 if team == teams[0] else 1

            summary["won"] = 1 if line_score.loc[line_score["team"] == team]["total"].iloc[0] > line_score.loc[line_score["team"] != team]["total"].iloc[0] else 0
            game["won"] = 1 if line_score.loc[line_score["team"] == team]["total"].iloc[0] > line_score.loc[line_score["team"] != team]["total"].iloc[0] else 0

            if base_cols is None:
                base_players_cols = summary.columns.drop_duplicates(keep="first")
                base_game_cols = game.columns.drop_duplicates(keep="first")

                base_game_cols = [b for b in base_game_cols if "bpm" not in b and "+/-" not in b and "unnamed" not in b]
                base_players_cols = [b for b in base_players_cols if "bpm" not in b and "unnamed" not in b]


            summary = summary[base_players_cols].loc[:, ~summary[base_players_cols].columns.duplicated()]
            game = game[base_game_cols].loc[:, ~game[base_game_cols].columns.duplicated()]


            player_summaries.append(summary.loc[summary.index != "Team Totals"])
            game_summaries.append(game) 

        player_summary = pd.concat(player_summaries)
        game_summary = pd.concat(game_summaries)

        columns_to_keep = [col for col in game_summary.columns if "team" not in col]

        game_summary_opp = game_summary[columns_to_keep].iloc[::-1]
        game_summary_opp.columns += "_opp"

        full_game = pd.concat([game_summary, game_summary_opp], axis=1)
        full_game["season"] = read_season_info(soup)
        full_game["date"] = os.path.basename(box_score)[5:13]
        full_game["date"] = pd.to_datetime(full_game["date"], format="%Y%m%d")
        player_summary["season"] = read_season_info(soup)
        player_summary["date"] = os.path.basename(box_score)[5:13]
        player_summary["date"] = pd.to_datetime(player_summary["date"], format="%Y%m%d")

        games_stats.append(full_game)
        players_stats.append(player_summary)
        
        if len(full_game.columns) != 140:
            print(f"{box_score}: {len(full_game.columns)}")
    
    return games_stats, players_stats

if __name__ == "__main__":

    data_dir = "D:\\Documentos\\GitHub\\Data-Analysis\\NBA"
    
    csv_games_dir = os.path.join(data_dir, "csv files", "games")
    csv_players_dir = os.path.join(data_dir, "csv files", "players")

    os.makedirs(csv_games_dir, exist_ok=True)
    os.makedirs(csv_players_dir, exist_ok=True)

    seasons = list(range(2000, 2011))

    for season in seasons:
        season_scores_dir = os.path.join(data_dir, "season scores", str(season))

        box_scores = os.listdir(season_scores_dir)
        box_scores = [os.path.join(season_scores_dir, f) for f in box_scores if f.endswith(".html")]

        games_stats, players_stats = get_file_stats(box_scores)
        
        games_stats_df = pd.concat(games_stats, ignore_index=True)
        players_stats_df = pd.concat(players_stats)

        save_path_games = os.path.join(csv_games_dir, f"nba_stats_games_{season}.csv")
        save_path_players = os.path.join(csv_players_dir, f"nba_stats_players_{season}.csv")

        players_stats_df.index.name = "Player"
        
        

        games_stats_df.to_csv(save_path_games, index=False)
        players_stats_df.to_csv(save_path_players)
