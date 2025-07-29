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
ACCOUNTS_FILE = "accounts.csv"

ALL_FILES = {
    USERS_FILE: ["chat_id", "username"],
    TRANSACTIONS_FILE: [
        "transaction_id", "chat_id", "timestamp", "amount", "account_id",
        "category", "description", "transaction_type", "tags"
    ],
    BALANCES_FILE: ["chat_id", "account_id", "balance", "currency", "date"],
    ACCOUNTS_FILE: ["account_id", "chat_id", "account_name", "account_type", "currency"]
}
CATEGORIES = ["Salary", "Groceries", "Entertainment", "Shopping", "Activities"]

# States for ConversationHandler
ACCOUNT_NAME, ACCOUNT_TYPE, CURRENCY, INITIAL_BALANCE = range(4)
ACCOUNT_SELECTION, TRANSACTION_AMOUNT, CATEGORY, DESCRIPTION = range(4)

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
    if not users_df["chat_id"].isin([int(chat_id)]).any():
        append_file(USERS_FILE, chat_id=chat_id, username=username)
        logger.info(f"Added user {username} with chat_id {chat_id}")
    else:
        logger.info("User already exists.")

def check_if_user_has_an_account(accounts_df, chat_id):
    user_accounts = accounts_df[accounts_df["chat_id"] == int(chat_id)]
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
            rf"Приветик {user.mention_html()}! Жмай /create_account чтобы создать новый счёт (это обязательно). "
            r"Жмай /add_transaction чтобы добавить транзакцию. "
            r"Жмай /analytics чтобы посмотреть аналитику своих расходов и накоплений (в разработке)."
        )
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")

async def start_create_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start the create_account conversation."""
    chat_id = str(update.effective_chat.id)
    await update.message.reply_text("Придумай имя для своего нового счёта:")
    return ACCOUNT_NAME

async def account_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle account name input and prompt for account type."""
    chat_id = str(update.effective_chat.id)
    account_name_input = update.message.text.strip()
    if not account_name_input:
        await update.message.reply_text("Имя счёта не может быть пустым. Подумай получше:")
        return ACCOUNT_NAME
    context.user_data['account_name'] = account_name_input

    # Prompt for account type
    keyboard = [
        [InlineKeyboardButton("Обычный", callback_data='usual'),
         InlineKeyboardButton("Сберегательный", callback_data='savings'),
         InlineKeyboardButton("Кредитный", callback_data='credit')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выбери тип счёта:", reply_markup=reply_markup)
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
    await query.message.reply_text("Выбери валюту:", reply_markup=reply_markup)
    return CURRENCY

async def currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle currency selection and save the account."""
    query = update.callback_query
    await query.answer()
    chat_id = str(update.effective_chat.id)
    selected_currency = query.data
    context.user_data['currency'] = selected_currency

    await update.callback_query.message.reply_text("Введи начальный баланс счёта:")
    return INITIAL_BALANCE

async def initial_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = str(update.effective_chat.id)
    initial_balance_input = update.message.text.strip()
    if not initial_balance_input:
        await update.message.reply_text("Введи правильный баланс, он должен быть из цифр:")
        return INITIAL_BALANCE

    try:
        initial_balance_input = float(initial_balance_input)
    except ValueError:
        await update.message.reply_text("Введи правильный баланс, он должен быть из цифр:")
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
            f"Счёт `{acc_name}` успешно создан! \U0001F498"
        )
        # Initialize balance for the new account
        append_file(
            BALANCES_FILE,
            chat_id=chat_id,
            account_id=account_id,
            balance=initial_balance_input,
            currency=selected_currency,
            date=datetime.now(timezone('UTC')).isoformat()
        )
    except Exception as e:
        logger.error(f"Error saving account for chat_id {chat_id}: {str(e)}")
        await update.message.reply_text("Не получилось :(")
        return ConversationHandler.END

    # Clear user_data
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the conversation."""
    await update.message.reply_text("Галя, отмена \U0001F628")
    context.user_data.clear()
    return ConversationHandler.END

def get_account_mappings(chat_id: str) -> dict:
    accounts_df = read_file(ACCOUNTS_FILE)
    has_account = check_if_user_has_an_account(accounts_df, chat_id)
    if not has_account:
        return {}
    mappings = {
        row['account_id']: {
            'account_name': row['account_name'],
            'account_type': row['account_type'],
            'currency': row['currency']
        }
        for _, row in accounts_df[accounts_df["chat_id"] == int(chat_id)].iterrows()
    }
    return mappings

async def start_add_transaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = str(update.effective_chat.id)
    accounts_df = read_file(ACCOUNTS_FILE)
    has_account = check_if_user_has_an_account(accounts_df, chat_id)
    message = update.message or update.edited_message
    if not has_account:
        await message.reply_text("У тебя пока нет ни одного счёта. :( Жмай /create_account !")
        return ConversationHandler.END

    account_mappings = get_account_mappings(chat_id)
    if not account_mappings:
        await update.message.reply_text("У тебя пока нет ни одного счёта. :( Жмай /create_account !")
        return ConversationHandler.END

    keyboard = []
    current_row = []
    for account_id, info in account_mappings.items():
        button_text = f"{info['account_name']} ({info['currency']})"
        button = InlineKeyboardButton(button_text, callback_data=str(account_id))
        current_row.append(button)
        if len(current_row) == 2:
            keyboard.append(current_row)
            current_row = []
    if current_row:
        keyboard.append(current_row)

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выбери нужный счёт:", reply_markup=reply_markup)
    return ACCOUNT_SELECTION

async def handle_account_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    account_id = query.data
    context.user_data['account_id'] = account_id

    # Retrieve account details
    chat_id = str(update.effective_chat.id)
    account_mappings = get_account_mappings(chat_id)
    account_info = account_mappings.get(int(account_id), {})

    await query.message.reply_text(
        f"Введи сумму транзакции (e.g., 50.25 or -50.25):"
    )
    return TRANSACTION_AMOUNT


async def handle_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount_text = update.message.text.strip()
    try:
        amount = float(amount_text)
        context.user_data['transaction_amount'] = amount

        # Create dynamic keyboard for categories (max 2 per row)
        keyboard = []
        current_row = []
        for idx, category in enumerate(CATEGORIES, 1):
            button = InlineKeyboardButton(category, callback_data=str(idx))
            current_row.append(button)
            if len(current_row) == 2:
                keyboard.append(current_row)
                current_row = []
        if current_row:
            keyboard.append(current_row)

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Выбери что больше подходит:", reply_markup=reply_markup)
        return CATEGORY
    except ValueError:
        await update.message.reply_text("Сумма транзакции должна быть цифровой:")
        return TRANSACTION_AMOUNT


async def handle_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data['category'] = query.data

    await query.message.reply_text("Введи описание транзакции, или 'none':")
    return DESCRIPTION


def update_balance(chat_id: str, account_id: int, amount: float, acc_currency: str):
    """
    Update balance in balances.csv for the given chat_id and account_id.
    Uses the latest balance entry for the account and adds the transaction amount.
    Appends a new balance entry without erasing existing data.

    Args:
        chat_id (str): The chat_id of the user.
        account_id (int): The account_id to update.
        amount (float): The transaction amount (positive for income, negative for expense).
        acc_currency (str): The currency of the account.
    """
    balances_df = read_file(BALANCES_FILE)

    # Find the latest balance entry for the chat_id and account_id
    if not balances_df.empty:
        existing_balances = balances_df[
            (balances_df['chat_id'] == int(chat_id)) &
            (balances_df['account_id'] == int(account_id))
            ]
        if not existing_balances.empty:
            # Select the latest entry based on full timestamp
            latest_balance = existing_balances.sort_values(by='date', ascending=False).iloc[0]
            current_balance = latest_balance['balance']
            new_balance = current_balance + amount
        else:
            # No prior balance for this account; start with the transaction amount
            new_balance = amount
    else:
        # No balances in the file; start with the transaction amount
        new_balance = amount

    # Append new balance entry
    try:
        append_file(
            BALANCES_FILE,
            chat_id=chat_id,
            account_id=account_id,
            balance=new_balance,
            currency=acc_currency,
            date=datetime.now(timezone('UTC')).isoformat()
        )
    except Exception as e:
        logger.error(f"Error appending balance for chat_id {chat_id}, account_id {account_id}: {str(e)}")
        raise
    return new_balance


async def handle_description(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    description = update.message.text.strip()
    if description.lower() == 'none':
        description = ""
    context.user_data['description'] = description

    # Retrieve transaction details
    chat_id = str(update.effective_chat.id)
    account_id = context.user_data['account_id']
    amount = context.user_data['transaction_amount']
    category = context.user_data['category']
    account_mappings = get_account_mappings(chat_id)
    account_info = account_mappings.get(int(account_id), {})

    # Save transaction to transactions.csv
    transactions_df = read_file(TRANSACTIONS_FILE)
    transaction_id = transactions_df['transaction_id'].max() + 1 if not transactions_df.empty else 1
    try:
        append_file(
            TRANSACTIONS_FILE,
            transaction_id=transaction_id,
            chat_id=chat_id,
            timestamp=datetime.now(timezone('UTC')).isoformat(),
            amount=amount,
            account_id=int(account_id),
            category=category,
            description=description,
            transaction_type="income" if amount > 0 else "expense",
            tags=""
        )
        # Update balance
        new_balance = update_balance(chat_id, int(account_id), amount, account_info['currency'])

        await update.message.reply_text(
            f"Теперь на твоем счету `{account_info['account_name']}` целых {new_balance} {account_info['currency']}! \U0001F970"
        )
    except Exception as e:
        logger.error(f"Error saving transaction for chat_id {chat_id}: {str(e)}")
        await update.message.reply_text("Что-то не вышло. :(")
        return ConversationHandler.END

    context.user_data.clear()
    return ConversationHandler.END

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

    transaction_handler = ConversationHandler(
        entry_points=[CommandHandler("add_transaction", start_add_transaction)],
        states={
            ACCOUNT_SELECTION: [CallbackQueryHandler(handle_account_selection)],
            TRANSACTION_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_amount)],
            CATEGORY: [CallbackQueryHandler(handle_category)],
            DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_description)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Add handlers
    application.add_handler(account_creation_handler)
    application.add_handler(transaction_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("analytics", show_analytics))

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()