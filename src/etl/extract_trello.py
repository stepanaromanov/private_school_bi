from src.utils.utils_dataframe import *
from src.etl.connect import *
import pandas as pd
import requests
from datetime import datetime
from configs.logging_config import get_logger
logger = get_logger("etl_log")


def trello_fetch_data(key, token, base_url = "https://api.trello.com/1"):
    """
    get card creation datetime from Trello card ID
    def trello_id_to_datetime(id):
        try:
            return datetime.fromtimestamp(int(id[:8], 16), datetime.UTC)
        except Exception:
            return pd.NaT
    """
    # ------------------------------------------------------
    # Safe fetch helper with try/except and graceful fallback
    # ------------------------------------------------------
    def fetch(url):
        params = {"key": key, "token": token}
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                logger.error(f"⚠️ JSON decode error for {url}")
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"⚠️ Request failed for {url}: {e}")
            return []
        except Exception as e:
            logger.error(f"⚠️ Unexpected error while fetching {url}: {e}")
            return []

    # ------------------------------------------------------
    # Fetch all boards
    # ------------------------------------------------------
    boards = fetch(f"{base_url}/members/me/boards")

    # Flatten basic info
    boards_df = pd.DataFrame([{
        "id": b.get("id"),
        "name": b.get("name"),
        "url": b.get("url"),
        "board_desc": b.get("desc"),
        "closed": b.get("closed"),
        "last_activity": b.get("dateLastActivity"),
        "last_view": b.get("dateLastView")
    } for b in boards])

    # ------------------------------------------------------
    # Fetch lists and cards for each board
    # ------------------------------------------------------
    all_lists = []
    all_cards = []

    for board in boards:
        board_id = board.get("id")

        # --- Lists ---
        lists = fetch(f"{base_url}/boards/{board_id}/lists")

        for lst in lists:
            all_lists.append({
                "board_id": board_id,
                "id": lst.get("id"),
                "list_name": lst.get("name"),
                "closed": lst.get("closed")
            })

        # --- Cards ---
        cards = fetch(f"{base_url}/boards/{board_id}/cards")

        for card in cards:
            all_cards.append({
                "board_id": board_id,
                "list_id": card.get("idList"),
                "id": card.get("id"),
                "card_name": card.get("name"),
                "card_desc": card.get("desc"),
                "card_labels": card.get("labels"),
                "due_date": card.get("due"),
                "card_due_complete": card.get("dueComplete"),
                "url": card.get("url"),
                "closed": card.get("closed"),
                # "created_at": trello_id_to_datetime(card.get("id"))
            })

    cards_df = pd.DataFrame(all_cards)
    lists_df = pd.DataFrame(all_lists)

    # ------------------------------------------------------
    # Fetch checklists for each card
    # ------------------------------------------------------
    all_checklists = []

    for _, card in cards_df.iterrows():
        card_id = card["id"]
        board_id = card["board_id"]

        checklists = fetch(f"{base_url}/cards/{card_id}/checklists")

        for checklist in checklists:
            for item in checklist.get("checkItems", []):
                all_checklists.append({
                    "board_id": board_id,
                    "card_id": card_id,
                    "id": checklist.get("id"),
                    "checklist_name": checklist.get("name"),
                    "item_name": item.get("name"),
                    "state": item.get("state")
                })

    checklists_df = pd.DataFrame(all_checklists)

    # --- Boards preprocessing  ---
    boards_df.fillna(0, inplace=True)
    boards_df = clean_string_columns(boards_df)
    boards_df = normalize_columns(boards_df)
    boards_df = add_timestamp(boards_df)
    boards_df.attrs["name"] = "trello_boards"
    save_df_with_timestamp(df=boards_df)

    # --- Cards preprocessing ---
    cards_df.fillna(0, inplace=True)
    cards_df = clean_string_columns(cards_df)
    cards_df = normalize_columns(cards_df)
    cards_df = add_timestamp(cards_df)
    cards_df.attrs["name"] = "trello_cards"
    save_df_with_timestamp(df=cards_df)

    # --- Lists preprocessing ---
    lists_df.fillna(0, inplace=True)
    lists_df = clean_string_columns(lists_df)
    lists_df = normalize_columns(lists_df)
    lists_df = add_timestamp(lists_df)
    lists_df.attrs["name"] = "trello_lists"
    save_df_with_timestamp(df=lists_df)

    # --- Checklists  preprocessing ---
    checklists_df.fillna(0, inplace=True)
    checklists_df = clean_string_columns(checklists_df)
    checklists_df = normalize_columns(checklists_df)
    checklists_df = add_timestamp(checklists_df)
    checklists_df.attrs["name"] = "trello_checklists"
    save_df_with_timestamp(df=checklists_df)

    return  boards_df, cards_df, checklists_df, lists_df