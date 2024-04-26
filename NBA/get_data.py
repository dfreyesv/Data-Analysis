import os
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import pandas as pd
import time, sys
import asyncio

# Function to get html section from page
# basketball-reference allows max 20 requests per min
async def get_html(url, selector, sleep=3.5, retries=3):
    
    html = None
    for i in range(1, retries+1):
        time.sleep(sleep * i)
        try:
            async with async_playwright() as p:
                browser = await p.firefox.launch()
                page = await browser.new_page()
                await page.goto(url)
                # print(await page.title())
                html = await page.inner_html(selector)
        except PlaywrightTimeout:
            print(f"Timeout error on {url}")
            continue
        else:
            break
    return html

async def scrape_games(season):
    url = f"https://www.basketball-reference.com/leagues/NBA_{season}_games.html"
    html = await get_html(url, "#content .filter")
    
    soup = BeautifulSoup(html, "html.parser")
    games = soup.find_all("a")
    season_month_games = [f"https://www.basketball-reference.com{l['href']}" for l in games]
    
    # Create the directory for the season if it doesn't exist
    season_dir = os.path.join(season_games_dir, str(season))
    os.makedirs(season_dir, exist_ok=True)
    
    # Initialize a counter for numbering files
    file_counter = 1
    
    for url in season_month_games:
        # Format the counter as a three-digit number with leading zeros
        file_number = f"{file_counter:03d}"
        save_path = os.path.join(season_dir, f"{file_number}_{url.split('/')[-1]}")
        
        # Increment the file counter
        file_counter += 1
        
        if os.path.exists(save_path):
            #print("skipped")
            continue
        
        html = await get_html(url, "#all_schedule")
        with open(save_path, "w+") as f:
            f.write(html)

async def scrape_scores(games_file, file_counter):
    with open(games_file, 'r') as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    scores = soup.find_all("a")
    hrefs = [l.get("href") for l in scores]
    box_scores = [l for l in hrefs if l and "boxscore" in l and ".html" in l]
    box_scores =  [f"https://www.basketball-reference.com{l}" for l in box_scores]
    
    # Create the directory for the season if it doesn't exist
    season_dir = os.path.join(season_scores_dir, games_file.split("\\")[-2])
    os.makedirs(season_dir, exist_ok=True)
    
    for url in box_scores:
        
        # Format the counter as a four-digit number with leading zeros
        file_number = f"{file_counter:04d}"
        
        save_path = os.path.join(season_dir, f"{file_number}_{url.split('/')[-1]}")
        
        # Increment the file counter
        file_counter += 1
        
        if os.path.exists(save_path):
            continue

        html = await get_html(url, "#content")
        if not html:
            continue
        with open(save_path, "w+", encoding="utf-8") as f:
                f.write(html)
    return file_counter


if __name__ == "__main__":
    # List of seasons scrap the games from.
    SEASONS = list(range(2000, 2011))

    # Directories where to save the html files
    data_dir = "D:\\Documentos\\GitHub\\Data-Analysis\\NBA"
    season_games_dir = os.path.join(data_dir, "season games")
    season_scores_dir = os.path.join(data_dir, "season scores")

    for season in SEASONS:
        asyncio.run(scrape_games(season))

    for season in SEASONS:
        season_dir = os.path.join(season_games_dir, str(season))
        games_files = os.listdir(season_dir)
        games_files = [x for x in games_files if ".html" in x]
        
        # Initialize a counter for numbering files
        file_counter = 1
        for f in games_files:
            filepath = os.path.join(season_dir, f)
        
            final_file_counter = asyncio.run(scrape_scores(filepath, file_counter))
            
            file_counter = final_file_counter
