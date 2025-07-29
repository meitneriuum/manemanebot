import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, time, timedelta
from io import BytesIO
from dotenv import load_dotenv
from pytz import timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters,
    CallbackQueryHandler, ConversationHandler
)

# Load environment variables
load_dotenv()
bot_api_key = os.getenv("BOT_API_KEY")

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# File paths
data_folder = "data"
USERS_FILE = "users.csv"
TRANSACTIONS_FILE = "transactions.csv"
BALANCES_FILE = "balances.csv"
CATEGORIES_FILE = "categories.csv"
ACCOUNTS_FILE = "accounts.csv"

ALL_FILES = {
    USERS_FILE: ["chat_id", "username"],
    TRANSACTIONS_FILE: [
        "transaction_id", "chat_id", "timestamp", "amount", "account_id",
        "category_id", "description", "transaction_type", "tags"
    ],
    BALANCES_FILE: ["chat_id", "account_id", "balance", "currency", "date"],
    CATEGORIES_FILE: ["category_id", "category_name"],
    ACCOUNTS_FILE: ["account_id", "chat_id", "account_name", "account_type", "currency"]
}

# States for ConversationHandler
ACCOUNT_NAME, ACCOUNT_TYPE, CURRENCY, INITIAL_BALANCE = range(4)

def get_file_path(filename: str) -> str:
    return os.path.join(data_folder, filename)

def init_csvs():
    os.makedirs(data_folder, exist_ok=True)
    for filename, columns in ALL_FILES.items():
        filepath = get_file_path(filename)
        if not os.path.exists(filepath):
            df = pd.DataFrame(columns=columns)
            df.to_csv(filepath, index=False)

def read_file(filename: str) -> pd.DataFrame:
    filepath = get_file_path(filename)
    df = pd.read_csv(filepath)
    return df

def append_file(filename, **kwargs):
    filepath = get_file_path(filename)
    values = [kwargs.get(column_nm, None) for column_nm in ALL_FILES[filename]]
    if any(v is None for v in values):
        raise ValueError(f"Not all kwargs were provided for {filename}.\nProvided kwargs: {kwargs}\nColumns expected: {ALL_FILES[filename]}")
    df = pd.DataFrame(data=[values], columns=ALL_FILES[filename])
    if os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
    else:
        df.to_csv(filepath, index=False)

def add_user(chat_id, username):
    users_df = read_file(USERS_FILE)
    if not users_df["chat_id"].isin([chat_id]).any():
        append_file(USERS_FILE, chat_id=chat_id, username=username)
        logger.info(f"Added user {username} with chat_id {chat_id}")
    else:
        logger.info("User already exists.")

def check_if_user_has_an_account(chat_id):
    accounts_df = read_file(ACCOUNTS_FILE)
    user_accounts = accounts_df[accounts_df["chat_id"] == chat_id]
    return not user_accounts.empty

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    username = user.full_name.replace(",", ";")
    chat_id = str(update.effective_chat.id)
    add_user(chat_id, username)
    try:
        message = update.message or update.edited_message
        if not message:
            logger.error(f"No valid message in start command for chat {chat_id}")
            return
        await message.reply_html(
            rf"Hi {user.mention_html()}! Use /create_account to add a new banking account. "
            r"Use /add_transaction to add a new transaction. "
            r"Use /analytics to see your balance and spending trends over time."
        )
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")

async def start_create_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the create_account conversation."""
    chat_id = str(update.effective_chat.id)
    await update.message.reply_text("Please enter the name for your new account:")
    return ACCOUNT_NAME

async def account_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle account name input and prompt for account type."""
    chat_id = str(update.effective_chat.id)
    account_name_input = update.message.text.strip()
    if not account_name_input:
        await update.message.reply_text("Account name cannot be empty. Please enter a valid name:")
        return ACCOUNT_NAME
    context.user_data['account_name'] = account_name_input

    # Prompt for account type
    keyboard = [
        [InlineKeyboardButton("Usual", callback_data='usual'),
         InlineKeyboardButton("Savings", callback_data='savings'),
         InlineKeyboardButton("Credit", callback_data='credit')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Choose the account type:", reply_markup=reply_markup)
    return ACCOUNT_TYPE

async def account_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle account type selection and prompt for currency."""
    query = update.callback_query
    await query.answer()
    selected_account_type = query.data
    context.user_data['account_type'] = selected_account_type

    # Prompt for currency
    keyboard = [
        [InlineKeyboardButton("BYN", callback_data='BYN'),
         InlineKeyboardButton("USD", callback_data='USD'),
         InlineKeyboardButton("EUR", callback_data='EUR')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.reply_text("Choose the currency:", reply_markup=reply_markup)
    return CURRENCY

async def currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle currency selection and save the account."""
    query = update.callback_query
    await query.answer()
    chat_id = str(update.effective_chat.id)
    selected_currency = query.data
    context.user_data['currency'] = selected_currency

    await update.message.reply_text("Please enter the initial balance for your account:")
    return INITIAL_BALANCE

async def initial_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = str(update.effective_chat.id)
    initial_balance_input = update.message.text.strip()
    if not initial_balance_input or not initial_balance_input.isdigit():
        await update.message.reply_text("Initial balance must be numeric. Please enter a valid value:")
        return INITIAL_BALANCE

    acc_name = context.user_data['account_name']
    selected_account_type = context.user_data['account_type']
    selected_currency = context.user_data['currency']

    # Generate unique account_id
    accounts_df = read_file(ACCOUNTS_FILE)
    account_id = accounts_df['account_id'].max() + 1 if not accounts_df.empty else 1

    # Save to accounts.csv
    try:
        append_file(
            ACCOUNTS_FILE,
            account_id=account_id,
            chat_id=chat_id,
            account_name=acc_name,
            account_type=selected_account_type,
            currency=selected_currency
        )
        await update.message.reply_text(
            f"Account created successfully!\n"
            f"Name: {acc_name}\nType: {selected_account_type}\nCurrency: {selected_currency}"
        )
        # Initialize balance for the new account
        append_file(
            BALANCES_FILE,
            chat_id=chat_id,
            account_id=account_id,
            balance=float(initial_balance_input),
            currency=selected_currency,
            date=datetime.now(timezone('UTC')).isoformat()
        )
    except Exception as e:
        logger.error(f"Error saving account for chat_id {chat_id}: {str(e)}")
        await update.message.reply_text("An error occurred while creating the account. Please try again.")
        return ConversationHandler.END

    # Clear user_data
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the conversation."""
    await update.message.reply_text("Operation cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

async def start_add_transaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = str(update.effective_chat.id)
    has_account = check_if_user_has_an_account(chat_id)
    if not has_account:
        message = update.message or update.edited_message
        await message.reply_text("You don't have an account yet. Please use /create_account to add a new banking account.")
    else:
        pass  # Implement transaction logic later

async def show_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = str(update.effective_chat.id)
    pass  # Implement analytics logic later

def main() -> None:
    init_csvs()

    try:
        application = Application.builder().token(bot_api_key).build()
        logger.info("Application initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Application: {str(e)}")
        return

    # Define ConversationHandler for account creation
    account_creation_handler = ConversationHandler(
        entry_points=[CommandHandler("create_account", start_create_account)],
        states={
            ACCOUNT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, account_name)],
            ACCOUNT_TYPE: [CallbackQueryHandler(account_type)],
            CURRENCY: [CallbackQueryHandler(currency)],
            INITIAL_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, initial_balance)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )


    # Add handlers
    application.add_handler(account_creation_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_transaction", start_add_transaction))
    application.add_handler(CommandHandler("analytics", show_analytics))

    application.run_polling(allowed_updates=Update.ALL_TYPES, poll_interval=10)

if __name__ == "__main__":
    main()