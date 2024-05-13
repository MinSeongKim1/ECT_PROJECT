from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

def get_match_probabilities(url, show_type):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url)
    time.sleep(10)  # Wait for dynamic content to load

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    if show_type == "live":
        # LIVE PROBABILITIES extraction
        print("\nLIVE PROBABILITIES:")
        live_probs_section = soup.select_one("div.perc-graph-ctr")
        if live_probs_section:
            for row in live_probs_section.select(".wdw-row"):
                team = row.select_one("span").text.strip() if row.select_one("span") else "Draw"
                probability = row.select_one(".prob-ctr").text.strip()
                print(f"{team}: {probability}")
        else:
            print("LIVE PROBABILITIES data not found.")

    elif show_type == "pre":
        # Win Probabilities from Match Results
        print("\nPRE-GAME PROBABILITIES:")
        win_probabilities_section = soup.select_one("h3:-soup-contains('Win Probabilities')")
        if win_probabilities_section:
            probabilities = win_probabilities_section.find_next('ul').find_all('li')
            for item in probabilities:
                team_or_draw, prob = item.text.strip().split(': ')
                print(f"{team_or_draw}: {prob}")
        else:
            print("Win Probabilities data not found.")

# Main Execution
week = input("Enter the match week (e.g., 34): ")
home_team = input("Enter the home team's abbreviation (e.g., lutn for Luton): ")
away_team = input("Enter the away team's abbreviation (e.g., bre for Brentford): ")
url = f"https://www.dimers.com/bet-hub/epl/schedule/2023_{week}_{home_team.lower()}_{away_team.lower()}"

game_status = input("Has the game been played? ('yes' for completed or in-progress, 'no' for not yet): ").lower()

if game_status == 'no':
    print("Predictions data is not available for non-played matches.")
else:
    user_choice = input("Would you like to see 'live' probabilities or 'pre'? Enter 'live' or 'pre': ").lower()
    get_match_probabilities(url, user_choice)
