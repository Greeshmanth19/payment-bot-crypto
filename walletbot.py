import os
import json
import logging
import requests
from datetime import datetime, timedelta
import calendar
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from web3 import Web3
from eth_account import Account
import secrets
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from croniter import croniter
import time
import re
from bson import ObjectId
import httpx
# Add this after your other imports
import asyncio


# Load environment variables
load_dotenv()

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables with error checking
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
INFURA_API_KEY = os.environ.get("INFURA_API_KEY")
MONGODB_URI = os.environ.get("MONGODB_URI")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "ethereum_wallet_bot")

if not TELEGRAM_TOKEN or not INFURA_API_KEY or not MONGODB_URI:
    raise ValueError("TELEGRAM_TOKEN, INFURA_API_KEY, and MONGODB_URI must be set in environment variables")

# Connect to Ethereum network using Infura
w3 = Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"))

# Connect to MongoDB
try:
    mongodb_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    # Ping the server to check connection
    mongodb_client.admin.command('ping')
    logger.info("Successfully connected to MongoDB")
    db = mongodb_client[MONGODB_DB_NAME]
    
    # Define collections
    wallets_collection = db["wallets"]
    username_mapping_collection = db["username_mapping"]
    pending_notifications_collection = db["pending_notifications"]
    scheduled_payments_collection = db["scheduled_payments"]
    
    # Create indexes for better performance
    wallets_collection.create_index("user_id", unique=True)
    username_mapping_collection.create_index("username", unique=True)
    pending_notifications_collection.create_index("username")
    scheduled_payments_collection.create_index("next_execution")
    
except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# Reusable keyboards
def back_to_menu_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]])

def create_main_menu_keyboard():
    """Create main menu keyboard with side-by-side buttons"""
    keyboard = [
        [
            InlineKeyboardButton("💼 Create Wallet", callback_data='create_wallet'),
            InlineKeyboardButton("🔑 Import Wallet", callback_data='import_wallet')
        ],
        [
            InlineKeyboardButton("💰 Check Balance", callback_data='check_balance'),
            InlineKeyboardButton("📋 Show Address", callback_data='show_address')
        ],
        [
            InlineKeyboardButton("💸 Send ETH", callback_data='start_payment'),
            InlineKeyboardButton("📊 Batch Payment", callback_data='batch_payment')
        ],
        [
            InlineKeyboardButton("⏰ Schedule Payment", callback_data='schedule_payment'),
            InlineKeyboardButton("🔄 Manage Scheduled", callback_data='manage_scheduled')
        ],
        [
            InlineKeyboardButton("💹 ETH Price", callback_data='check_price'),
            InlineKeyboardButton("ℹ️ Help", callback_data='help')
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


# MongoDB data operations
def get_wallet(user_id):
    """Get wallet for a user from MongoDB"""
    return wallets_collection.find_one({"user_id": user_id})

def save_wallet(user_id, wallet_data):
    """Save or update wallet in MongoDB"""
    wallet_data["user_id"] = user_id
    wallets_collection.update_one(
        {"user_id": user_id},
        {"$set": wallet_data},
        upsert=True
    )

def get_all_wallets():
    """Get all wallets from MongoDB"""
    cursor = wallets_collection.find({})
    wallets = {}
    for doc in cursor:
        user_id = doc.pop("user_id")  # Remove user_id from the document
        wallets[user_id] = doc
    return wallets

def update_username_mapping(user_id, username):
    """Update username mapping in MongoDB"""
    if not username:
        return
        
    username_mapping_collection.update_one(
        {"username": username.lower()},
        {"$set": {"user_id": str(user_id)}},
        upsert=True
    )

def get_user_id_by_username(username):
    """Get user_id by username from MongoDB"""
    result = username_mapping_collection.find_one({"username": username.lower()})
    return result["user_id"] if result else None

def get_wallet_by_username(username):
    """Find wallet by username using MongoDB"""
    # First check the username mapping
    user_id = get_user_id_by_username(username.lower())
    if user_id:
        wallet = get_wallet(user_id)
        if wallet:
            return user_id, wallet
    
    # If not found, check all wallets
    cursor = wallets_collection.find({"username": {"$regex": f"^{username}$", "$options": "i"}})
    for wallet in cursor:
        user_id = wallet.pop("user_id")
        return user_id, wallet
            
    return None, None

def get_pending_notifications(username):
    """Get pending notifications for a username"""
    result = pending_notifications_collection.find_one({"username": username.lower()})
    return result["notifications"] if result and "notifications" in result else []

def save_pending_notification(username, notification):
    """Save a pending notification"""
    pending_notifications_collection.update_one(
        {"username": username.lower()},
        {"$push": {"notifications": notification}},
        upsert=True
    )

def remove_pending_notification(username, notification_id):
    """Remove a pending notification by its ID"""
    pending_notifications_collection.update_one(
        {"username": username.lower()},
        {"$pull": {"notifications": {"_id": notification_id}}}
    )


def save_scheduled_payment(payment_data):
    """Save a scheduled payment to MongoDB with consistent ID type"""
    try:
        # Make a deep copy to avoid modifying the original data unintentionally
        payment_to_save = payment_data.copy()
        
        # CRITICAL: Always ensure sender_id is string
        if "sender_id" in payment_to_save:
            payment_to_save["sender_id"] = str(payment_to_save["sender_id"])
        
        # Print debugging information
        logger.info(f"Saving scheduled payment with sender_id: {payment_to_save.get('sender_id')}")
        logger.info(f"Payment data type check - sender_id type: {type(payment_to_save.get('sender_id'))}")
        
        # Insert payment into collection
        result = scheduled_payments_collection.insert_one(payment_to_save)
        payment_id = result.inserted_id
        
        # Verify the payment was saved with direct query
        saved_payment = scheduled_payments_collection.find_one({"_id": payment_id})
        
        if not saved_payment:
            logger.error(f"CRITICAL ERROR: Payment with ID {payment_id} not found after saving!")
            return payment_id
        
        # Verify sender_id was saved correctly
        saved_sender_id = saved_payment.get("sender_id")
        logger.info(f"Saved payment verification - found with ID {payment_id}")
        logger.info(f"Saved payment sender_id: {saved_sender_id}, type: {type(saved_sender_id)}")
        
        # Verify we can find this payment by sender_id
        sender_id_payments = list(scheduled_payments_collection.find({"sender_id": saved_sender_id}))
        logger.info(f"Found {len(sender_id_payments)} payments with sender_id: {saved_sender_id}")
        
        return payment_id
        
    except Exception as e:
        logger.error(f"Error in save_scheduled_payment: {e}")
        # Re-raise to make sure the calling function is aware
        raise


def get_scheduled_payments(user_id):
    """Get all scheduled payments for a user with robust error handling"""
    try:
        # Always ensure user_id is string
        str_user_id = str(user_id)
        logger.info(f"Getting scheduled payments for user_id: {str_user_id}")
        
        # DEBUG: Print all payments in the collection
        all_payments = list(scheduled_payments_collection.find({}))
        logger.info(f"DEBUG: Total payments in database: {len(all_payments)}")
        for payment in all_payments:
            payment_id = payment.get("_id")
            payment_sender = payment.get("sender_id")
            sender_type = type(payment_sender)
            logger.info(f"DEBUG: Payment {payment_id}: sender_id={payment_sender}, type={sender_type}")
        
        # First attempt: direct string comparison query
        user_payments = list(scheduled_payments_collection.find({"sender_id": str_user_id}))
        logger.info(f"Direct query found {len(user_payments)} payments")
        
        # Second attempt: regex search (in case of string/non-string mismatch)
        if not user_payments:
            logger.info(f"Trying regex search for sender_id: {str_user_id}")
            user_payments = list(scheduled_payments_collection.find(
                {"sender_id": {"$regex": f"^{str_user_id}$"}}
            ))
            logger.info(f"Regex query found {len(user_payments)} payments")
        
        # Third attempt: manual filter (fallback)
        if not user_payments:
            logger.info(f"Trying manual filter for sender_id: {str_user_id}")
            user_payments = []
            for payment in all_payments:
                payment_sender = payment.get("sender_id")
                if payment_sender and str(payment_sender) == str_user_id:
                    user_payments.append(payment)
                    logger.info(f"Manually matched payment: {payment.get('_id')}")
            
            logger.info(f"Manual filter found {len(user_payments)} payments")
        
        return user_payments
            
    except Exception as e:
        logger.error(f"Error in get_scheduled_payments: {e}")
        # Return empty list on error
        return []

def format_wallet_info(address, private_key):
    """Format wallet information with better styling"""
    return (
        f"**Address:**\n"
        f"`{address}`\n\n"
        f"**Private Key:**\n"
        f"`{private_key}`\n\n"
        f"*⚠️ Keep your private key safe and never share it with anyone. "
        f"Losing your private key may lead to loss of funds in your wallet.*\n\n"
        f"*⚠️ Neither us nor Telegram store your private keys so you can use these wallets "
        f"securely just make sure to delete the message after storing the private key.*"
    )

def get_all_due_scheduled_payments():
    """Get all scheduled payments that are due for execution"""
    now = datetime.now()
    payments = list(scheduled_payments_collection.find({"next_execution": {"$lte": now}, "active": True}))
    
    logger.info(f"Found {len(payments)} due scheduled payments")
    for payment in payments:
        logger.info(f"Due payment: id={payment['_id']}, sender_id={payment.get('sender_id')}, type={type(payment.get('sender_id'))}")
    
    return payments

def update_scheduled_payment(payment_id, update_data):
    """Update a scheduled payment"""
    scheduled_payments_collection.update_one(
        {"_id": payment_id},
        {"$set": update_data}
    )

def delete_scheduled_payment(payment_id):
    """Delete a scheduled payment"""
    scheduled_payments_collection.delete_one({"_id": payment_id})

def get_eth_price():
    """Get current ETH price in USD"""
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd')
        data = response.json()
        return data['ethereum']['usd']
    except Exception as e:
        logger.error(f"Error fetching ETH price: {e}")
        return None

def calculate_optimal_gas():
    """Calculate optimal gas parameters for transaction"""
    try:
        # Get current base fee
        latest_block = w3.eth.get_block('latest')
        base_fee = latest_block.get('baseFeePerGas', w3.eth.gas_price)
        
        # Get suggested priority fee (tip)
        priority_fee = w3.eth.max_priority_fee
        
        # Add small buffer to priority fee (1 Gwei)
        priority_fee = priority_fee + w3.to_wei(1, 'gwei')
        
        # Calculate max fee per gas (base fee * 1.5 + priority fee)
        max_fee_per_gas = int(base_fee * 1.5 + priority_fee)
        
        # Cap at 100 Gwei
        max_fee_per_gas = min(max_fee_per_gas, w3.to_wei(100, 'gwei'))
        
        return {
            'maxFeePerGas': max_fee_per_gas,
            'maxPriorityFeePerGas': priority_fee,
            'gasLimit': 21000  # Standard gas limit for simple transfers
        }
    except Exception as e:
        logger.warning(f"Error calculating optimal gas, using fallback: {e}")
        # Fallback to legacy gas calculation
        gas_price = int(w3.eth.gas_price * 1.2)  # 20% premium
        return {
            'gasPrice': gas_price,
            'gasLimit': 21000
        }

def parse_schedule_string(schedule_string):
    """Parse user input schedule to a cron expression or next date
    
    Supports:
    - Every [day of week]
    - Every [number] days
    - DD-MM-YY or DD-MM-YYYY (specific date)
    """
    schedule_string = schedule_string.lower().strip()
    
    # Check for weekday schedule
    for i, day in enumerate(['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']):
        if f"every {day}" in schedule_string:
            # Cron expression: minute hour * * day_of_week
            # day_of_week is 0-6 where 0 is Sunday in cron
            # Convert to cron weekday (0=Sunday, 1=Monday, etc.)
            cron_weekday = (i + 1) % 7  # Convert to cron format (0=Sunday)
            return f"0 12 * * {cron_weekday}", "weekly"
    
    # Check for "every X days" pattern
    every_days_match = re.search(r'every\s+(\d+)\s+days?', schedule_string)
    if every_days_match:
        days = int(every_days_match.group(1))
        now = datetime.now()
        next_date = now + timedelta(days=days)
        return next_date, "periodic", days
    
    # Check for specific date (DD-MM-YY or DD-MM-YYYY)
    date_match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})', schedule_string)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        year = int(date_match.group(3))
        
        # Handle 2-digit year
        if year < 100:
            current_year = datetime.now().year
            century = current_year // 100
            year = century * 100 + year
        
        try:
            scheduled_date = datetime(year, month, day, 12, 0)  # Noon on the specified day
            return scheduled_date, "one-time"
        except ValueError:
            return None, None
    
    return None, None

def calculate_next_execution(schedule_type, schedule_value):
    """Calculate the next execution time based on schedule"""
    now = datetime.now()
    
    if schedule_type == "weekly":
        # schedule_value is a cron expression
        cron = croniter(schedule_value, now)
        return cron.get_next(datetime)
    
    elif schedule_type == "periodic":
        # schedule_value[0] is the next date, schedule_value[1] is the period in days
        if isinstance(schedule_value, tuple):
            return schedule_value[0]
        else:
            return now + timedelta(days=schedule_value)
    
    elif schedule_type == "one-time":
        # schedule_value is a datetime object
        return schedule_value
    
    return None
async def check_pending_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check and send pending notifications for this user with improved formatting"""
    user = update.effective_user
    if not user.username:
        return
        
    username = user.username.lower()
    notifications = get_pending_notifications(username)
    
    if not notifications:
        return
    
    # Process all pending notifications
    for notification in notifications:
        try:
            notification_id = notification.get("_id", str(datetime.now().timestamp()))
            
            if notification["type"] == "received_eth":
                # Ensure transaction hash has 0x prefix
                tx_hash = notification.get("tx_hash", "")
                if tx_hash and not tx_hash.startswith("0x"):
                    tx_hash = "0x" + tx_hash
                else:
                    tx_hash = tx_hash or "Unknown"
                
                if notification.get("new_wallet", False):
                    # Format wallet info with improved formatting
                    wallet_address = notification.get('wallet_address', 'Unknown')
                    private_key = notification.get('private_key', 'Unknown')
                    
                    await context.bot.send_message(
                        chat_id=user.id,
                        text=f"🎉 Good news! @{notification.get('sender_username', 'Someone')} sent you {notification.get('amount', 'some')} ETH!\n\n"
                            f"A wallet was automatically created for you:\n\n"
                            + format_wallet_info(wallet_address, private_key) + 
                            f"\nTransaction: https://etherscan.io/tx/{tx_hash}",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("Check Balance", callback_data='check_balance'),
                                InlineKeyboardButton("Send ETH", callback_data='start_payment')
                            ]
                        ])
                    )
                else:
                    # Create button row
                    button_row = [
                        InlineKeyboardButton("Check Balance", callback_data='check_balance'),
                        InlineKeyboardButton("Send ETH", callback_data='start_payment')
                    ]
                    
                    await context.bot.send_message(
                        chat_id=user.id,
                        text=f"💰 You received {notification.get('amount', 'some')} ETH from @{notification.get('sender_username', 'Someone')}!\n\n"
                            f"Transaction: https://etherscan.io/tx/{tx_hash}",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup([button_row])
                    )
                
                # Remove sent notification
                remove_pending_notification(username, notification_id)
                
        except Exception as e:
            logger.error(f"Error sending pending notification: {e}")

# Command and callback handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message when the command /start is issued."""
    user = update.effective_user
    
    # Store username in context for future use
    if user.username:
        update_username_mapping(user.id, user.username)
        
        wallet = get_wallet(str(user.id))
        if wallet:
            wallet["username"] = user.username
            save_wallet(str(user.id), wallet)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    await update.message.reply_text(
        f"Welcome {user.first_name} to your Ethereum Wallet Assistant! 🚀\n\n"
        f"Choose an option from the menu below:",
        reply_markup=create_main_menu_keyboard()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send help message."""
    # Update username mapping if available
    if update.effective_user.username:
        update_username_mapping(update.effective_user.id, update.effective_user.username)
        
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    await update.message.reply_text(
        f"Ethereum Wallet Assistant Commands:\n\n"
        f"/start - Show main menu\n"
        f"/create - Create a new wallet\n"
        f"/import [private key] - Import an existing wallet\n"
        f"/balance - Check your wallet balance\n"
        f"/address - Show your wallet address\n"
        f"/pay [@username/address] [amount] - Send ETH\n"
        f"/price - Check current ETH price\n"
        f"/schedule [@username/address] [amount] [schedule] - Schedule a payment\n"
        f"/batchpay [@username1,@username2] [amount] - Send same amount to multiple recipients\n"
        f"/batchpaymulti [@username1:amount1,@username2:amount2] - Send different amounts to multiple recipients",
        reply_markup=create_main_menu_keyboard()
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button clicks with debounce protection"""
    query = update.callback_query
    
    # Debounce protection
    current_time = time.time()
    last_action_time = context.user_data.get('last_action_time', 0)
    
    if current_time - last_action_time < 2:  # 2 seconds debounce
        await query.answer("Please wait a moment before clicking again.")
        return
    
    # Update last action time
    context.user_data['last_action_time'] = current_time
    
    await query.answer()
    
    # Update username mapping in case it changed
    if query.from_user.username:
        update_username_mapping(query.from_user.id, query.from_user.username)
    
    # Process the button click
    if query.data == 'create_wallet':
        await create_wallet(update, context)
    elif query.data == 'import_wallet':
        async def edit_action():
            return await query.edit_message_text(
                "Please send your private key in the format:\n/import 0x...\n\n"
                "⚠️ Never share your private key with anyone else!",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data == 'check_balance':
        await check_balance(update, context)
    elif query.data == 'show_address':
        await get_address(update, context)
    elif query.data == 'start_payment':
        async def edit_action():
            return await query.edit_message_text(
                "Send ETH in the format:\n"
                "/pay @username 0.1\n"
                "or\n"
                "/pay 0xAddress 0.1",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data == 'schedule_payment':
        async def edit_action():
            return await query.edit_message_text(
                "Schedule a payment in one of these formats:\n\n"
                "/schedule @username 0.01 every monday\n"
                "/schedule 0xAddress 0.05 every 7 days\n"
                "/schedule @username 0.1 25-12-2025\n\n"
                "Your payment will be processed automatically at the specified time.",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data == 'manage_scheduled':
        # Add debugging output
        user_id = str(query.from_user.id)
        logger.info(f"manage_scheduled button clicked by user_id: {user_id}")
        
        # Direct database check for debugging
        all_payments = list(scheduled_payments_collection.find({}))
        for payment in all_payments:
            pay_sender_id = payment.get('sender_id')
            logger.info(f"Payment in DB: sender_id={pay_sender_id}, matches user={pay_sender_id == user_id}")
        
        await manage_scheduled_payments(update, context)
    elif query.data == 'batch_payment':
        async def edit_action():
            return await query.edit_message_text(
                "Send batch payments in one of these formats:\n\n"
                "Same amount to multiple recipients:\n"
                "/batchpay @user1,@user2,0xAddress 0.01\n\n"
                "Different amounts to different recipients:\n"
                "/batchpaymulti @user1:0.01,@user2:0.02,0xAddress:0.03",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data == 'check_price':
        await check_eth_price(update, context)
    elif query.data == 'help':
        async def edit_action():
            return await query.edit_message_text(
                f"Ethereum Wallet Assistant Commands:\n\n"
                f"/start - Show main menu\n"
                f"/create - Create a new wallet\n"
                f"/import [private key] - Import an existing wallet\n"
                f"/balance - Check your wallet balance\n"
                f"/address - Show your wallet address\n"
                f"/pay [@username/address] [amount] - Send ETH\n"
                f"/price - Check current ETH price\n"
                f"/schedule [@username/address] [amount] [schedule] - Schedule a payment\n"
                f"/batchpay [@username1,@username2] [amount] - Send same amount to multiple recipients\n"
                f"/batchpaymulti [@username1:amount1,@username2:amount2] - Send different amounts",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data == 'main_menu':
        async def edit_action():
            return await query.edit_message_text(
                "Choose an option from the menu below:",
                reply_markup=create_main_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    elif query.data.startswith('cancel_scheduled_'):
        payment_id = ObjectId(query.data.split('_')[-1])
        # Update the scheduled payment to inactive
        update_scheduled_payment(payment_id, {"active": False})
        async def edit_action():
            return await query.edit_message_text(
                "✅ Scheduled payment has been cancelled.",
                reply_markup=back_to_menu_keyboard()
            )
        await retry_telegram_action(edit_action)
    else:
        async def edit_action():
            return await query.edit_message_text(
                "Unknown command. Please try again.",
                reply_markup=create_main_menu_keyboard()
            )
        await retry_telegram_action(edit_action)


async def create_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create a new Ethereum wallet with details saved in chat history."""
    # Handle both direct command and callback query
    if update.callback_query:
        user_id = str(update.callback_query.from_user.id)
        username = update.callback_query.from_user.username
        query = update.callback_query
        chat_id = query.message.chat_id
    else:
        user_id = str(update.effective_user.id)
        username = update.effective_user.username
        query = None
        chat_id = update.effective_chat.id
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    if not query:
        await check_pending_notifications(update, context)
    
    # Check if user already has a wallet
    existing_wallet = get_wallet(user_id)
    if existing_wallet:
        message = 'You already have a wallet. Use the Show Address option to see your wallet address.'
        buttons = [
            [
                InlineKeyboardButton("Show Address", callback_data='show_address'),
                InlineKeyboardButton("Check Balance", callback_data='check_balance')
            ],
            [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
        ]
        
        if query:
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(buttons))
        return
    
    # Let the user know we're processing their request
    if query:
        await query.answer("Creating your wallet...")
        await query.edit_message_text("Creating your wallet... Please wait.")
    
    # Generate a new wallet
    private_key = "0x" + secrets.token_hex(32)
    account = Account.from_key(private_key)
    
    # Store wallet with username if available
    wallet_data = {
        "address": account.address,
        "private_key": private_key,
        "created_at": datetime.now().isoformat()
    }
    
    if username:
        wallet_data["username"] = username
        
    save_wallet(user_id, wallet_data)
    
    # Format wallet information message
    wallet_info_message = (
        f"✅ Wallet created successfully!\n\n"
        f"**Address:**\n"
        f"`{account.address}`\n\n"
        f"**Private Key:**\n"
        f"`{private_key}`\n\n"
        f"*⚠️ Keep your private key safe and never share it with anyone. "
        f"Losing your private key may lead to loss of funds in your wallet.*\n\n"
        f"*⚠️ Neither us nor Telegram store your private keys so you can use these wallets "
        f"securely just make sure to delete the message after storing the private key.*"
    )
    
    # Send wallet details as a separate message (without buttons)
    # This will stay in chat history but can be deleted by the user
    await context.bot.send_message(
        chat_id=chat_id,
        text=wallet_info_message,
        parse_mode='Markdown'
    )
    
    # Send a new message with buttons for continuing interaction
    continue_message = "What would you like to do next?"
    buttons = [
        [
            InlineKeyboardButton("Check Balance", callback_data='check_balance'),
            InlineKeyboardButton("Send ETH", callback_data='start_payment')
        ],
        [InlineKeyboardButton("Main Menu", callback_data='main_menu')]
    ]
    
    # Send as a fresh message
    await context.bot.send_message(
        chat_id=chat_id,
        text=continue_message,
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    
    # If this was from a callback query, update the original message
    if query:
        try:
            await query.edit_message_text("✅ Wallet created successfully!")
        except Exception as e:
            logger.error(f"Could not update original message: {e}")
async def import_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Import an existing wallet using private key with debounce protection."""
    user_id = str(update.effective_user.id)
    username = update.effective_user.username
    
    # Debounce protection
    current_time = time.time()
    last_action_time = context.user_data.get('last_action_time', 0)
    
    if current_time - last_action_time < 2:  # 2 seconds debounce
        await update.message.reply_text("Please wait a moment before trying again.")
        return
    
    # Update last action time
    context.user_data['last_action_time'] = current_time
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    # Parse arguments
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            'Please provide a valid private key: /import [your-private-key]',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    private_key = context.args[0]
    
    # Send processing message
    processing_message = await update.message.reply_text(
        "Importing your wallet... Please wait.",
        reply_markup=None
    )
    
    # Add a small delay
    await asyncio.sleep(0.5)
    
    # Validate private key format
    if not private_key.startswith('0x') or len(private_key) != 66:
        await processing_message.edit_text(
            'Please provide a valid private key format (0x + 64 hexadecimal characters)',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    try:
        # Create account from private key
        account = Account.from_key(private_key)
        
        # Store wallet
        wallet_data = {
            "address": account.address,
            "private_key": private_key,
            "imported_at": datetime.now().isoformat()
        }
        
        if username:
            wallet_data["username"] = username
            
        save_wallet(user_id, wallet_data)
        
        # Format wallet info with better styling
        message = "✅ Wallet imported successfully!\n\n" + format_wallet_info(account.address, private_key)
        
        # Add buttons
        button_row = [
            InlineKeyboardButton("Check Balance", callback_data='check_balance'),
            InlineKeyboardButton("Send ETH", callback_data='start_payment')
        ]
        keyboard = [button_row, [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]]
        
        await processing_message.edit_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error importing wallet: {e}")
        await processing_message.edit_text(
            'Error importing wallet. Please check your private key and try again.',
            reply_markup=back_to_menu_keyboard()
        )

def with_debounce(func):
    """Decorator to add debounce protection to command handlers"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        # Debounce protection
        current_time = time.time()
        last_action_time = context.user_data.get('last_action_time', 0)
        
        if current_time - last_action_time < 2:  # 2 seconds debounce
            # Too frequent request
            if update.callback_query:
                await update.callback_query.answer("Please wait a moment before trying again.")
                return
            else:
                await update.message.reply_text("Please wait a moment before trying again.")
                return
        
        # Update last action time
        context.user_data['last_action_time'] = current_time
        
        # Call the original function
        return await func(update, context, *args, **kwargs)
    
    return wrapper

async def get_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user's wallet address."""
    # Handle both direct command and callback query
    if update.callback_query:
        user_id = str(update.callback_query.from_user.id)
        username = update.callback_query.from_user.username
        query = update.callback_query
    else:
        user_id = str(update.effective_user.id)
        username = update.effective_user.username
        query = None
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    if not query:
        await check_pending_notifications(update, context)
    
    wallet = get_wallet(user_id)
    
    if not wallet:
        message = "You don't have a wallet yet. Create one first."
        keyboard = [[InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                  [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]]
        
        if query:
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    address = wallet["address"]
    message = f"Your wallet address is:\n\n`{address}`"
    
    if query:
        await query.edit_message_text(message, reply_markup=back_to_menu_keyboard(), parse_mode='Markdown')
    else:
        await update.message.reply_text(message, reply_markup=back_to_menu_keyboard(), parse_mode='Markdown')

async def check_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check user's wallet balance."""
    # Handle both direct command and callback query
    if update.callback_query:
        user_id = str(update.callback_query.from_user.id)
        username = update.callback_query.from_user.username
        query = update.callback_query
    else:
        user_id = str(update.effective_user.id)
        username = update.effective_user.username
        query = None
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    if not query:
        await check_pending_notifications(update, context)
    
    wallet = get_wallet(user_id)
    
    if not wallet:
        message = "You don't have a wallet yet. Create one first."
        keyboard = [[InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                  [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]]
        
        if query:
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        return
    
    address = wallet["address"]
    
    try:
        # Get balance from Ethereum network
        balance_wei = w3.eth.get_balance(address)
        balance_eth = w3.from_wei(balance_wei, 'ether')
        
        # Get ETH price
        eth_price = get_eth_price()
        
        message = f"Your wallet balance:\n\n`{balance_eth:.6f} ETH`"
        if eth_price:
            usd_value = float(balance_eth) * eth_price
            message += f"\n\nValue: ${usd_value:.2f} USD"
        
        if query:
            await query.edit_message_text(message, reply_markup=back_to_menu_keyboard(), parse_mode='Markdown')
        else:
            await update.message.reply_text(message, reply_markup=back_to_menu_keyboard(), parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error checking balance: {e}")
        message = f"Error checking balance: {str(e)}. Please try again later."
        
        if query:
            await query.edit_message_text(message, reply_markup=back_to_menu_keyboard())
        else:
            await update.message.reply_text(message, reply_markup=back_to_menu_keyboard())

async def check_eth_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check current ETH price."""
    # Handle both direct command and callback query
    if update.callback_query:
        user_id = str(update.callback_query.from_user.id)
        username = update.callback_query.from_user.username
        query = update.callback_query
    else:
        user_id = str(update.effective_user.id)
        username = update.effective_user.username
        query = None
        
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    if not query:
        await check_pending_notifications(update, context)
    
    eth_price = get_eth_price()
    
    if eth_price:
        message = f"💹 Current ETH Price: ${eth_price:.2f} USD"
    else:
        message = "Sorry, couldn't fetch ETH price at the moment. Please try again later."
    
    if query:
        await query.edit_message_text(message, reply_markup=back_to_menu_keyboard())
    else:
        await update.message.reply_text(message, reply_markup=back_to_menu_keyboard())

async def schedule_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Schedule an ETH payment with improved debugging."""
    user_id = str(update.effective_user.id)
    username = update.effective_user.username
    
    logger.info(f"Scheduling payment for user_id: {user_id}, username: {username}")
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    wallet = get_wallet(user_id)
    
    if not wallet:
        await update.message.reply_text(
            "You don't have a wallet yet. Create one first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
            ])
        )
        return
    
    # Parse arguments
    if not context.args or len(context.args) < 3:
        await update.message.reply_text(
            'Usage: /schedule [@username or address] [amount] [schedule]\n\n'
            'Examples:\n/schedule @username 0.01 every monday\n'
            '/schedule 0xAddress 0.05 every 7 days\n'
            '/schedule @username 0.1 25-12-2023',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    recipient = context.args[0]
    amount_str = context.args[1]
    schedule_str = ' '.join(context.args[2:])
    
    logger.info(f"Schedule parameters: recipient={recipient}, amount={amount_str}, schedule={schedule_str}")
    
    # Validate amount
    try:
        amount = float(amount_str)
        amount_wei = w3.to_wei(amount, 'ether')
    except ValueError:
        await update.message.reply_text(
            'Invalid amount. Please enter a valid number.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Parse the schedule
    schedule_value, schedule_type, *extra = parse_schedule_string(schedule_str) or (None, None)
    
    if not schedule_value or not schedule_type:
        await update.message.reply_text(
            'Invalid schedule format. Please use one of these formats:\n'
            '- every monday/tuesday/etc.\n'
            '- every X days\n'
            '- DD-MM-YYYY',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    logger.info(f"Parsed schedule: type={schedule_type}, value={schedule_value}, extra={extra}")
    
    # Calculate the next execution time
    next_execution = calculate_next_execution(schedule_type, schedule_value if schedule_type != "periodic" else extra[0])
    
    if not next_execution:
        await update.message.reply_text(
            'Could not determine the next execution time. Please check your schedule format.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    logger.info(f"Next execution time: {next_execution}")
    
    # Get sender's wallet info
    from_address = wallet["address"]
    
    # Check if recipient is a username or an address
    new_private_key = None  # Initialize for storing private key of new wallets
    
    if recipient.startswith('@'):
        # It's a username
        username_to_send = recipient.lstrip('@')
        
        # Check if it's own username
        if username_to_send.lower() == update.effective_user.username.lower():
            await update.message.reply_text(
                "You can't schedule payments to yourself!",
                reply_markup=back_to_menu_keyboard()
            )
            return
            
        user_id_recipient, recipient_wallet = get_wallet_by_username(username_to_send)
        
        if not recipient_wallet:
            # Create a wallet for this user
            new_private_key = "0x" + secrets.token_hex(32)
            new_account = Account.from_key(new_private_key)
            recipient_address = new_account.address
            is_new_wallet = True
            
            # If we have a user ID, save this wallet
            if user_id_recipient:
                wallet_data = {
                    "address": recipient_address,
                    "private_key": new_private_key,
                    "created_at": datetime.now().isoformat(),
                    "username": username_to_send
                }
                save_wallet(user_id_recipient, wallet_data)
                logger.info(f"Created new wallet for recipient {username_to_send}: {recipient_address}")
            else:
                logger.info(f"Created temporary wallet for unknown user {username_to_send}: {recipient_address}")
        else:
            recipient_address = recipient_wallet["address"]
            is_new_wallet = False
            logger.info(f"Using existing wallet for {username_to_send}: {recipient_address}")
            
        display_name = f"@{username_to_send}"
    else:
        # It's an address
        recipient_address = recipient
        display_name = recipient
        is_new_wallet = False
        
        # Validate Ethereum address
        if not w3.is_address(recipient_address):
            await update.message.reply_text(
                'Invalid Ethereum address format. Please check the address and try again.',
                reply_markup=back_to_menu_keyboard()
            )
            return
    
    # Create the scheduled payment
    payment_data = {
        "sender_id": str(user_id),  # CRITICAL: Always use string for sender_id
        "sender_address": from_address,
        "recipient_address": recipient_address,
        "recipient_display": display_name,
        "amount": amount,
        "amount_wei": amount_wei,
        "schedule_type": schedule_type,
        "schedule_value": schedule_value if schedule_type != "periodic" else extra[0],
        "next_execution": next_execution,
        "created_at": datetime.now(),
        "active": True,
        "is_new_wallet": is_new_wallet
    }
    
    # Add private key for new wallets
    if is_new_wallet and new_private_key:
        payment_data["private_key"] = new_private_key
    
    logger.info(f"Preparing to save scheduled payment: recipient={display_name}, "
               f"amount={amount}, sender_id={payment_data['sender_id']}")
    
    # Save to database
    try:
        payment_id = save_scheduled_payment(payment_data)
        logger.info(f"Successfully saved scheduled payment with ID: {payment_id}")
        
        # Verify that we can retrieve this payment
        verification_payments = get_scheduled_payments(user_id)
        logger.info(f"Verification: Found {len(verification_payments)} payments for user {user_id}")
        
        # Double-check if this payment exists in the verification list
        found = False
        for p in verification_payments:
            if str(p.get("_id")) == str(payment_id):
                found = True
                logger.info(f"Verification successful: Found the newly created payment in the user's payments")
                break
        
        if not found:
            logger.error(f"VERIFICATION FAILED: Newly created payment not found in user's payments!")
            # Continue execution anyway, but log the failure
    
    except Exception as e:
        logger.error(f"Error saving scheduled payment: {e}")
        await update.message.reply_text(
            'Error saving your scheduled payment. Please try again later.',
            reply_markup=back_to_menu_keyboard()
        )
        return

    # Format next execution time
    next_exec_str = next_execution.strftime("%A, %B %d, %Y at %I:%M %p")
    
    # Format schedule description
    if schedule_type == "weekly":
        weekday = next_execution.strftime("%A")
        schedule_desc = f"every {weekday}"
    elif schedule_type == "periodic":
        days = extra[0]
        schedule_desc = f"every {days} days"
    else:  # one-time
        schedule_desc = f"one time on {next_exec_str}"
    
    # Confirmation message
    confirmation_message = (
        f"✅ Payment scheduled successfully!\n\n"
        f"To: {display_name}\n"
        f"Amount: {amount} ETH\n"
        f"Schedule: {schedule_desc}\n"
        f"Next execution: {next_exec_str}"
    )
    
    # DEBUG: Add payment ID to confirmation message for debugging
    confirmation_message += f"\n\nInternal payment ID: {payment_id}"
    
    await update.message.reply_text(
        confirmation_message,
        reply_markup=back_to_menu_keyboard()
    )


async def manage_scheduled_payments(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Manage user's scheduled payments with robust error handling and debugging"""
    try:
        # Handle both direct command and callback query
        if update.callback_query:
            user_id = str(update.callback_query.from_user.id)
            username = update.callback_query.from_user.username
            query = update.callback_query
        else:
            user_id = str(update.effective_user.id)
            username = update.effective_user.username
            query = None
        
        logger.info(f"Managing scheduled payments for user_id: {user_id}")
        
        # Update username mapping if available
        if username:
            update_username_mapping(user_id, username)
        
        # Check for pending notifications
        if not query:
            await check_pending_notifications(update, context)
        
        # Get user's scheduled payments
        payments = get_scheduled_payments(user_id)
        logger.info(f"Retrieved {len(payments)} payments for user {user_id}")
        
        # DEBUG: Dump detailed payment info
        for i, payment in enumerate(payments):
            try:
                payment_id = payment.get("_id")
                payment_sender = payment.get("sender_id")
                payment_recipient = payment.get("recipient_display", "Unknown")
                payment_amount = payment.get("amount", 0)
                payment_active = payment.get("active", True)
                
                logger.info(f"Payment {i+1}: id={payment_id}, sender={payment_sender}, "
                            f"recipient={payment_recipient}, amount={payment_amount}, active={payment_active}")
            except Exception as e:
                logger.error(f"Error logging payment details: {e}")
        
        # Filter active payments
        active_payments = []
        for p in payments:
            try:
                if p.get("active", True):  # Default to True if missing
                    active_payments.append(p)
            except Exception as e:
                logger.error(f"Error checking payment active status: {e}")
        
        logger.info(f"Active payments: {len(active_payments)}")
        
        if not active_payments or len(active_payments) == 0:
            message = "You don't have any scheduled payments."
            
            if query:
                await query.edit_message_text(message, reply_markup=back_to_menu_keyboard())
            else:
                await update.message.reply_text(message, reply_markup=back_to_menu_keyboard())
            return
        
        # Generate message with list of payments
        message = "Your scheduled payments:\n\n"
        keyboard = []
        
        for i, payment in enumerate(active_payments, 1):
            try:
                # Get values with safe defaults
                recipient = payment.get("recipient_display", "Unknown")
                amount = payment.get("amount", 0)
                next_execution = payment.get("next_execution", datetime.now())
                
                # Convert next_execution to datetime if needed
                if not isinstance(next_execution, datetime):
                    try:
                        if isinstance(next_execution, str):
                            next_execution = datetime.fromisoformat(next_execution.replace('Z', '+00:00'))
                        else:
                            next_execution = datetime.now()
                    except Exception:
                        next_execution = datetime.now()
                
                next_exec_str = next_execution.strftime("%a, %b %d, %Y")
                
                # Determine schedule description safely
                schedule_type = payment.get("schedule_type", "one-time")
                if schedule_type == "weekly":
                    try:
                        weekday = next_execution.strftime("%A")
                        schedule_desc = f"every {weekday}"
                    except Exception:
                        schedule_desc = "weekly"
                elif schedule_type == "periodic":
                    try:
                        days = payment.get("schedule_value", "?")
                        schedule_desc = f"every {days} days"
                    except Exception:
                        schedule_desc = "periodic"
                else:  # one-time or unknown
                    schedule_desc = "one time payment"
                
                # Add to message
                message += f"{i}. To: {recipient}\n"
                message += f"   Amount: {amount} ETH\n"
                message += f"   Schedule: {schedule_desc}\n"
                message += f"   Next execution: {next_exec_str}\n\n"
                
                # Add cancel button
                payment_id = payment.get("_id")
                if payment_id:
                    keyboard.append([InlineKeyboardButton(f"Cancel payment #{i}", callback_data=f"cancel_scheduled_{payment_id}")])
            except Exception as e:
                logger.error(f"Error processing payment {i}: {e}")
                # Skip this payment but continue with others
                continue
        
        # Add back button
        keyboard.append([InlineKeyboardButton("Back to Main Menu", callback_data="main_menu")])
        
        if query:
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
    
    except Exception as e:
        # Catch-all error handler
        logger.error(f"Unexpected error in manage_scheduled_payments: {e}")
        message = "Sorry, something went wrong. Please try again later."
        
        if query:
            await query.edit_message_text(message, reply_markup=back_to_menu_keyboard())
        else:
            await update.message.reply_text(message, reply_markup=back_to_menu_keyboard())


async def batch_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the same amount of ETH to multiple recipients."""
    user_id = str(update.effective_user.id)
    username = update.effective_user.username
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    wallet = get_wallet(user_id)
    
    if not wallet:
        await update.message.reply_text(
            "You don't have a wallet yet. Create one first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
            ])
        )
        return
    
    # Parse arguments
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            'Usage: /batchpay [@user1,@user2,0xAddress...] [amount]\n\n'
            'Example: /batchpay @user1,@user2,0xAddress 0.01',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    recipients_str = context.args[0]
    amount_str = context.args[1]
    
    # Validate amount
    try:
        amount = float(amount_str)
        amount_wei = w3.to_wei(amount, 'ether')
    except ValueError:
        await update.message.reply_text(
            'Invalid amount. Please enter a valid number.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Split recipients
    recipient_list = recipients_str.split(',')
    
    if len(recipient_list) < 1:
        await update.message.reply_text(
            'Please provide at least one recipient.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Process recipients
    processed_recipients = []
    
    for recipient in recipient_list:
        recipient = recipient.strip()
        
        if recipient.startswith('@'):
            # It's a username
            username_to_send = recipient.lstrip('@')
            
            # Check if it's own username
            if username_to_send.lower() == username.lower():
                await update.message.reply_text(
                    f"You can't send ETH to yourself! Skipping {recipient}.",
                    reply_markup=back_to_menu_keyboard()
                )
                continue
                
            user_id_recipient, recipient_wallet = get_wallet_by_username(username_to_send)
            
            if not recipient_wallet:
                # Create a wallet for this user
                new_private_key = "0x" + secrets.token_hex(32)
                new_account = Account.from_key(new_private_key)
                recipient_address = new_account.address
                is_new_wallet = True
                
                # FIX: Save new wallet if user_id is found
                if user_id_recipient:
                    # Store wallet with username
                    wallet_data = {
                        "address": recipient_address,
                        "private_key": new_private_key,
                        "created_at": datetime.now().isoformat(),
                        "username": username_to_send
                    }
                    save_wallet(user_id_recipient, wallet_data)
            else:
                recipient_address = recipient_wallet["address"]
                is_new_wallet = False
                
            display_name = f"@{username_to_send}"
        else:
            # It's an address
            recipient_address = recipient
            display_name = recipient
            is_new_wallet = False
            
            # Validate Ethereum address
            if not w3.is_address(recipient_address):
                await update.message.reply_text(
                    f'Invalid Ethereum address format: {recipient}. Skipping.',
                    reply_markup=back_to_menu_keyboard()
                )
                continue
        
        # Add to processed list
        processed_recipients.append({
            "address": recipient_address,
            "display": display_name,
            "amount": amount,
            "amount_wei": amount_wei,
            "is_new_wallet": is_new_wallet,
            "private_key": new_private_key if is_new_wallet else None,  # FIX: Store private key for new wallets
            "username": username_to_send if recipient.startswith('@') else None
        })
    
    if len(processed_recipients) == 0:
        await update.message.reply_text(
            'No valid recipients found.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Check sender's balance
    from_address = wallet["address"]
    balance_wei = w3.eth.get_balance(from_address)
    
    # Calculate gas
    gas_params = calculate_optimal_gas()
    
    # For legacy transactions
    if 'gasPrice' in gas_params:
        gas_price = gas_params['gasPrice']
        estimated_gas_cost_wei = gas_price * gas_params['gasLimit']
    else:  # For EIP-1559 transactions
        gas_price = gas_params['maxFeePerGas']
        estimated_gas_cost_wei = gas_params['maxFeePerGas'] * gas_params['gasLimit']
    
    # Calculate total cost
    total_amount_wei = sum(r["amount_wei"] for r in processed_recipients)
    total_gas_wei = estimated_gas_cost_wei * len(processed_recipients)
    total_cost_wei = total_amount_wei + total_gas_wei
    
    # Check if balance can cover amount + gas
    if balance_wei < total_cost_wei:
        await update.message.reply_text(
            f'❌ Insufficient balance for these transactions + gas fees. You need approximately '
            f'{w3.from_wei(total_cost_wei, "ether")} ETH in total, but have '
            f'{w3.from_wei(balance_wei, "ether")} ETH.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Ask for confirmation
    eth_price = get_eth_price()
    
    confirmation_message = (
        f"🔄 Confirm batch transaction:\n\n"
        f"From: `{from_address}`\n"
        f"Number of recipients: {len(processed_recipients)}\n"
        f"Amount per recipient: {amount} ETH\n"
        f"Total amount: {w3.from_wei(total_amount_wei, 'ether')} ETH\n"
    )
    
    if eth_price:
        usd_value = float(w3.from_wei(total_amount_wei, 'ether')) * eth_price
        confirmation_message += f"Total value: ${usd_value:.2f} USD\n"
        
    gas_cost_eth = w3.from_wei(total_gas_wei, 'ether')
    confirmation_message += f"Estimated total gas: {gas_cost_eth:.6f} ETH\n"
    
    if eth_price:
        gas_usd = float(gas_cost_eth) * eth_price
        confirmation_message += f"Gas cost: ${gas_usd:.2f} USD\n"
    
    confirmation_message += f"\nRecipients:\n"
    
    # List recipients in confirmation
    for i, recipient in enumerate(processed_recipients, 1):
        if i <= 5:
            confirmation_message += f"{i}. {recipient['display']}: {recipient['amount']} ETH\n"
        elif i == 6:
            confirmation_message += f"... and {len(processed_recipients) - 5} more\n"
    
    # Store batch payment info in context
    context.user_data["batch_payment"] = {
        "from_address": from_address,
        "private_key": wallet["private_key"],
        "recipients": processed_recipients,
        "total_amount_wei": total_amount_wei,
        "total_gas_wei": total_gas_wei
    }
    
    # Create unique callback data
    callback_data = f"confirm_batch_{int(time.time())}"
    
    await update.message.reply_text(
        confirmation_message,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Confirm Batch Payment", callback_data=callback_data)],
            [InlineKeyboardButton("Cancel", callback_data="main_menu")]
        ])
    )

async def batch_payment_multi(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send different amounts of ETH to multiple recipients."""
    user_id = str(update.effective_user.id)
    username = update.effective_user.username
    
    # Update username mapping if available
    if username:
        update_username_mapping(user_id, username)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    wallet = get_wallet(user_id)
    
    if not wallet:
        await update.message.reply_text(
            "You don't have a wallet yet. Create one first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
            ])
        )
        return
    
    # Parse arguments
    if not context.args or len(context.args) < 1:
        await update.message.reply_text(
            'Usage: /batchpaymulti [@user1:0.01,@user2:0.02,0xAddress:0.03]\n\n'
            'Example: /batchpaymulti @user1:0.01,@user2:0.02,0xAddress:0.03',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    batch_str = context.args[0]
    
    # Split recipients
    recipient_pairs = batch_str.split(',')
    
    if len(recipient_pairs) < 1:
        await update.message.reply_text(
            'Please provide at least one recipient with amount.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Process recipients
    processed_recipients = []
    
    for pair in recipient_pairs:
        pair = pair.strip()
        
        # Split recipient and amount
        parts = pair.split(':')
        if len(parts) != 2:
            await update.message.reply_text(
                f'Invalid format for {pair}. Use recipient:amount format.',
                reply_markup=back_to_menu_keyboard()
            )
            continue
            
        recipient = parts[0].strip()
        amount_str = parts[1].strip()
        
        # Validate amount
        try:
            amount = float(amount_str)
            amount_wei = w3.to_wei(amount, 'ether')
        except ValueError:
            await update.message.reply_text(
                f'Invalid amount for {recipient}: {amount_str}. Skipping.',
                reply_markup=back_to_menu_keyboard()
            )
            continue
        
        if recipient.startswith('@'):
            # It's a username
            username_to_send = recipient.lstrip('@')
            
            # Check if it's own username
            if username_to_send.lower() == username.lower():
                await update.message.reply_text(
                    f"You can't send ETH to yourself! Skipping {recipient}.",
                    reply_markup=back_to_menu_keyboard()
                )
                continue
                
            user_id_recipient, recipient_wallet = get_wallet_by_username(username_to_send)
            
            if not recipient_wallet:
                # Create a wallet for this user
                new_private_key = "0x" + secrets.token_hex(32)
                new_account = Account.from_key(new_private_key)
                recipient_address = new_account.address
                is_new_wallet = True
            else:
                recipient_address = recipient_wallet["address"]
                is_new_wallet = False
                
            display_name = f"@{username_to_send}"
        else:
            # It's an address
            recipient_address = recipient
            display_name = recipient
            is_new_wallet = False
            
            # Validate Ethereum address
            if not w3.is_address(recipient_address):
                await update.message.reply_text(
                    f'Invalid Ethereum address format: {recipient}. Skipping.',
                    reply_markup=back_to_menu_keyboard()
                )
                continue
        
        # Add to processed list
        processed_recipients.append({
            "address": recipient_address,
            "display": display_name,
            "amount": amount,
            "amount_wei": amount_wei,
            "is_new_wallet": is_new_wallet
        })
    
    if len(processed_recipients) == 0:
        await update.message.reply_text(
            'No valid recipients found.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Check sender's balance
    from_address = wallet["address"]
    balance_wei = w3.eth.get_balance(from_address)
    
    # Calculate gas
    gas_params = calculate_optimal_gas()
    
    # For legacy transactions
    if 'gasPrice' in gas_params:
        gas_price = gas_params['gasPrice']
        estimated_gas_cost_wei = gas_price * gas_params['gasLimit']
    else:  # For EIP-1559 transactions
        gas_price = gas_params['maxFeePerGas']
        estimated_gas_cost_wei = gas_params['maxFeePerGas'] * gas_params['gasLimit']
    
    # Calculate total cost
    total_amount_wei = sum(r["amount_wei"] for r in processed_recipients)
    total_gas_wei = estimated_gas_cost_wei * len(processed_recipients)
    total_cost_wei = total_amount_wei + total_gas_wei
    
    # Check if balance can cover amount + gas
    if balance_wei < total_cost_wei:
        await update.message.reply_text(
            f'❌ Insufficient balance for these transactions + gas fees. You need approximately '
            f'{w3.from_wei(total_cost_wei, "ether")} ETH in total, but have '
            f'{w3.from_wei(balance_wei, "ether")} ETH.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Ask for confirmation
    eth_price = get_eth_price()
    
    confirmation_message = (
        f"🔄 Confirm multi-amount batch transaction:\n\n"
        f"From: `{from_address}`\n"
        f"Number of recipients: {len(processed_recipients)}\n"
        f"Total amount: {w3.from_wei(total_amount_wei, 'ether')} ETH\n"
    )
    
    if eth_price:
        usd_value = float(w3.from_wei(total_amount_wei, 'ether')) * eth_price
        confirmation_message += f"Total value: ${usd_value:.2f} USD\n"
        
    gas_cost_eth = w3.from_wei(total_gas_wei, 'ether')
    confirmation_message += f"Estimated total gas: {gas_cost_eth:.6f} ETH\n"
    
    if eth_price:
        gas_usd = float(gas_cost_eth) * eth_price
        confirmation_message += f"Gas cost: ${gas_usd:.2f} USD\n"
    
    confirmation_message += f"\nRecipients:\n"
    
    # List recipients in confirmation
    for i, recipient in enumerate(processed_recipients, 1):
        if i <= 5:
            confirmation_message += f"{i}. {recipient['display']}: {recipient['amount']} ETH\n"
        elif i == 6:
            confirmation_message += f"... and {len(processed_recipients) - 5} more\n"
    
    # Store batch payment info in context
    context.user_data["batch_payment_multi"] = {
        "from_address": from_address,
        "private_key": wallet["private_key"],
        "recipients": processed_recipients,
        "total_amount_wei": total_amount_wei,
        "total_gas_wei": total_gas_wei
    }
    
    # Create unique callback data
    callback_data = f"confirm_batch_multi_{int(time.time())}"
    
    await update.message.reply_text(
        confirmation_message,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Confirm Multi Batch Payment", callback_data=callback_data)],
            [InlineKeyboardButton("Cancel", callback_data="main_menu")]
        ])
    )



async def pay(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send ETH to an address or username."""
    sender_id = str(update.effective_user.id)
    sender_username = update.effective_user.username
    
    # Update username mapping if available
    if sender_username:
        update_username_mapping(sender_id, sender_username)
    
    # Check for pending notifications
    await check_pending_notifications(update, context)
    
    wallet = get_wallet(sender_id)
    
    if not wallet:
        await update.message.reply_text(
            "You don't have a wallet yet. Create one first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
            ])
        )
        return
    
    # Parse arguments
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            'Usage: /pay [@username or address] [amount]\n\nExample: /pay @username 0.01',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    recipient = context.args[0]
    amount_str = context.args[1]
    
    # Validate amount
    try:
        amount = float(amount_str)
        amount_wei = w3.to_wei(amount, 'ether')
    except ValueError:
        await update.message.reply_text(
            'Invalid amount. Please enter a valid number.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Get sender's wallet info
    from_address = wallet["address"]
    private_key = wallet["private_key"]
    
    # Check if recipient is a username or an address
    if recipient.startswith('@'):
        # It's a username
        username = recipient.lstrip('@')
        
        # Check if it's own username
        if username.lower() == sender_username.lower():
            await update.message.reply_text(
                "You can't send ETH to yourself!",
                reply_markup=back_to_menu_keyboard()
            )
            return
            
        user_id, recipient_wallet = get_wallet_by_username(username)
        
        if not recipient_wallet:
            # Create a wallet for this user
            new_private_key = "0x" + secrets.token_hex(32)
            new_account = Account.from_key(new_private_key)
            recipient_address = new_account.address
            
            # Prepare notification for when they join
            notification = {
                "_id": str(datetime.now().timestamp()),
                "type": "received_eth",
                "amount": amount,
                "sender_username": sender_username or "Unknown",
                "wallet_address": recipient_address,
                "private_key": new_private_key,
                "new_wallet": True,
                "timestamp": datetime.now().isoformat()
            }
            
            is_new_wallet = True
        else:
            recipient_address = recipient_wallet["address"]
            is_new_wallet = False
            
            # Prepare notification
            notification = {
                "_id": str(datetime.now().timestamp()),
                "type": "received_eth",
                "amount": amount,
                "sender_username": sender_username or "Unknown",
                "new_wallet": False,
                "timestamp": datetime.now().isoformat()
            }
            
        display_name = f"@{username}"
    else:
        # It's an address
        recipient_address = recipient
        display_name = recipient
        is_new_wallet = False
        
        # Validate Ethereum address
        if not w3.is_address(recipient_address):
            await update.message.reply_text(
                'Invalid Ethereum address format. Please check the address and try again.',
                reply_markup=back_to_menu_keyboard()
            )
            return
    
    # Check sender's balance
    balance_wei = w3.eth.get_balance(from_address)
    
    # Calculate gas
    gas_params = calculate_optimal_gas()
    
    # For legacy transactions
    if 'gasPrice' in gas_params:
        gas_price = gas_params['gasPrice']
        estimated_gas_cost_wei = gas_price * gas_params['gasLimit']
    else:  # For EIP-1559 transactions
        gas_price = gas_params['maxFeePerGas']
        estimated_gas_cost_wei = gas_params['maxFeePerGas'] * gas_params['gasLimit']
    
    # Check if balance can cover amount + gas
    if balance_wei < (amount_wei + estimated_gas_cost_wei):
        await update.message.reply_text(
            f'❌ Insufficient balance for this transaction + gas fees. You need approximately '
            f'{w3.from_wei(amount_wei + estimated_gas_cost_wei, "ether")} ETH in total, but have '
            f'{w3.from_wei(balance_wei, "ether")} ETH.',
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    # Ask for confirmation
    eth_price = get_eth_price()
    confirmation_message = (
        f"🔄 Confirm transaction:\n\n"
        f"From: `{from_address}`\n"
        f"To: {display_name}\n"
        f"Amount: {amount} ETH"
    )
    
    if eth_price:
        usd_value = amount * eth_price
        confirmation_message += f" (≈ ${usd_value:.2f} USD)"
        
    gas_cost_eth = w3.from_wei(estimated_gas_cost_wei, 'ether')
    confirmation_message += f"\nEstimated gas fee: {gas_cost_eth:.6f} ETH"
    
    if eth_price:
        gas_usd = float(gas_cost_eth) * eth_price
        confirmation_message += f" (≈ ${gas_usd:.2f} USD)"
    
    # Store payment info in context
    context.user_data["payment"] = {
        "from_address": from_address,
        "private_key": private_key,
        "to_address": recipient_address,
        "display_name": display_name,
        "amount": amount,
        "amount_wei": amount_wei,
        "is_new_wallet": is_new_wallet,
        "notification": notification if recipient.startswith('@') else None
    }
    
    # Create unique callback data
    callback_data = f"confirm_payment_{int(time.time())}"
    
    await update.message.reply_text(
        confirmation_message,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Confirm Payment", callback_data=callback_data)],
            [InlineKeyboardButton("Cancel", callback_data="main_menu")]
        ])
    )

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process confirmed payment."""
    query = update.callback_query
    await query.answer()
    
    # Get payment info from context
    if "payment" not in context.user_data:
        await query.edit_message_text(
            "Error: Payment data not found. Please try again.",
            reply_markup=back_to_menu_keyboard()
        )
        return
        
    payment = context.user_data["payment"]
    
    # Send a processing message
    await query.edit_message_text(
        "Processing payment...\nThis may take a moment. Please wait.",
        reply_markup=None
    )
    
    try:
        # Get transaction parameters
        from_address = payment["from_address"]
        private_key = payment["private_key"]
        to_address = payment["to_address"]
        amount_wei = payment["amount_wei"]
        display_name = payment["display_name"]
        is_new_wallet = payment["is_new_wallet"]
        notification = payment.get("notification")
        
        # Calculate gas
        gas_params = calculate_optimal_gas()
        
        # For legacy transactions
        if 'gasPrice' in gas_params:
            tx = {
                'nonce': w3.eth.get_transaction_count(from_address),
                'to': to_address,
                'value': amount_wei,
                'gas': gas_params['gasLimit'],
                'gasPrice': gas_params['gasPrice'],
                'chainId': 1  # Mainnet
            }
        else:  # For EIP-1559 transactions
            tx = {
                'nonce': w3.eth.get_transaction_count(from_address),
                'to': to_address,
                'value': amount_wei,
                'gas': gas_params['gasLimit'],
                'maxFeePerGas': gas_params['maxFeePerGas'],
                'maxPriorityFeePerGas': gas_params['maxPriorityFeePerGas'],
                'chainId': 1,  # Mainnet
                'type': 2  # EIP-1559
            }
        
        # Sign and send transaction
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()
        
        # Ensure tx_hash has 0x prefix
        if not tx_hash_hex.startswith("0x"):
            tx_hash_hex = "0x" + tx_hash_hex
        
        # Build successful message
        message = (
            f"✅ Payment sent successfully!\n\n"
            f"Transaction Hash: `{tx_hash_hex}`\n"
            f"Amount: {payment['amount']} ETH\n"
            f"Recipient: {display_name}\n\n"
            f"View on Etherscan: https://etherscan.io/tx/{tx_hash_hex}"
        )
        
        if is_new_wallet:
            message += f"\n\nA wallet was created for {display_name}."
        
        # Send success message
        await query.edit_message_text(
            message,
            reply_markup=back_to_menu_keyboard(),
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
        
        # Update notification with tx hash if needed
        if notification:
            notification["tx_hash"] = tx_hash_hex
            
            # Add to pending notifications for recipient
            username = display_name.lstrip('@')
            save_pending_notification(username, notification)
        
    except Exception as e:
        logger.error(f"Error sending transaction: {e}")
        
        await query.edit_message_text(
            f"❌ Error sending transaction: {str(e)}\n\nPlease try again later.",
            reply_markup=back_to_menu_keyboard()
        )
    finally:
        # Clear payment data
        context.user_data.pop("payment", None)


async def process_batch_transaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process confirmed batch transactions."""
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data
    
    if callback_data.startswith("confirm_batch_multi_"):
        # Multi-amount batch transaction
        if "batch_payment_multi" not in context.user_data:
            await query.edit_message_text(
                "Error: Batch payment data not found. Please try again.",
                reply_markup=back_to_menu_keyboard()
            )
            return
            
        batch_data = context.user_data["batch_payment_multi"]
        recipients = batch_data["recipients"]
        is_multi = True
    elif callback_data.startswith("confirm_batch_"):
        # Same-amount batch transaction
        if "batch_payment" not in context.user_data:
            await query.edit_message_text(
                "Error: Batch payment data not found. Please try again.",
                reply_markup=back_to_menu_keyboard()
            )
            return
            
        batch_data = context.user_data["batch_payment"]
        recipients = batch_data["recipients"]
        is_multi = False
    else:
        await query.edit_message_text(
            "Invalid callback data. Please try again.",
            reply_markup=back_to_menu_keyboard()
        )
        return
    
    user_id = str(query.from_user.id)
    wallet = get_wallet(user_id)
    
    if not wallet:
        await query.edit_message_text(
            "Wallet not found. Please create a wallet first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Create Wallet", callback_data='create_wallet')],
                [InlineKeyboardButton("Back to Main Menu", callback_data='main_menu')]
            ])
        )
        return
        
    from_address = wallet["address"]
    private_key = wallet["private_key"]
    
    # Send a processing message
    await query.edit_message_text(
        f"Processing batch payment with {len(recipients)} transactions...\n"
        f"This may take a moment. Please wait.",
        reply_markup=None
    )
    
    # Process each transaction
    results = []
    
    for i, recipient in enumerate(recipients, 1):
        try:
            recipient_address = recipient["address"]
            amount_wei = recipient["amount_wei"]
            display_name = recipient["display"]
            is_new_wallet = recipient.get("is_new_wallet", False)
            recipient_private_key = recipient.get("private_key")
            
            # Get nonce for this transaction
            nonce = w3.eth.get_transaction_count(from_address) + i - 1
            
            # Get gas parameters
            gas_params = calculate_optimal_gas()
            
            # Prepare transaction
            if 'gasPrice' in gas_params:
                # Legacy transaction
                tx = {
                    'nonce': nonce,
                    'to': recipient_address,
                    'value': amount_wei,
                    'gas': gas_params['gasLimit'],
                    'gasPrice': gas_params['gasPrice'],
                    'chainId': 1  # Mainnet
                }
            else:
                # EIP-1559 transaction
                tx = {
                    'nonce': nonce,
                    'to': recipient_address,
                    'value': amount_wei,
                    'gas': gas_params['gasLimit'],
                    'maxFeePerGas': gas_params['maxFeePerGas'],
                    'maxPriorityFeePerGas': gas_params['maxPriorityFeePerGas'],
                    'chainId': 1,  # Mainnet
                    'type': 2  # EIP-1559
                }
            
            # Sign and send transaction
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            logger.info(f"Transaction {i} sent: {tx_hash.hex()}")
            
            # Add to results (without private keys)
            results.append({
                "recipient": display_name,
                "amount": recipient["amount"],
                "tx_hash": tx_hash.hex(),
                "status": "success",
                "is_new_wallet": is_new_wallet,
                "recipient_address": recipient_address
            })
            
            # Send notification to recipient if it's a username
            if display_name.startswith('@'):
                username = display_name.lstrip('@')
                
                # Include wallet data for new users
                notification = {
                    "_id": str(datetime.now().timestamp()),
                    "type": "received_eth",
                    "amount": recipient["amount"],
                    "sender_username": query.from_user.username or "Unknown",
                    "tx_hash": tx_hash.hex(),
                    "timestamp": datetime.now().isoformat(),
                    "new_wallet": is_new_wallet
                }
                
                # Add wallet information for new users
                if is_new_wallet and recipient_private_key:
                    notification["wallet_address"] = recipient_address
                    notification["private_key"] = recipient_private_key
                
                # Add to pending notifications
                save_pending_notification(username, notification)
            
        except Exception as e:
            logger.error(f"Error in transaction {i}: {e}")
            
            # Add failed transaction to results
            results.append({
                "recipient": display_name,
                "amount": recipient["amount"],
                "tx_hash": None,
                "status": "failed",
                "error": str(e)
            })
    
    # Build result message
    result_message = f"✅ Batch transaction results ({len(results)} payments):\n\n"
    
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = len(results) - success_count
    
    result_message += f"Successful: {success_count}\n"
    result_message += f"Failed: {failed_count}\n\n"
    
    # Show details of each transaction
    if len(results) <= 10:
        for i, result in enumerate(results, 1):
            if result["status"] == "success":
                tx_hash = result["tx_hash"]
                # Ensure 0x prefix for transaction hash
                if not tx_hash.startswith("0x"):
                    tx_hash = "0x" + tx_hash
                
                result_message += f"{i}. To {result['recipient']}: {result['amount']} ETH ✅\n"
                result_message += f"   TX: https://etherscan.io/tx/{tx_hash}\n"
                
                # Add note about new wallet WITHOUT showing private key
                if result.get("is_new_wallet"):
                    result_message += f"   ⚠️ New wallet created for {result['recipient']}.\n"
                    result_message += f"\n   The recipient will receive their wallet details when they interact with the bot.\n"
                
                result_message += "\n"
            else:
                result_message += f"{i}. To {result['recipient']}: {result['amount']} ETH ❌\n"
                result_message += f"   Error: {result.get('error', 'Unknown error')}\n\n"
    else:
        # Just show summary for large batches
        result_message += f"Too many transactions to display individually.\n"
        result_message += f"Check your wallet transaction history on Etherscan for details.\n\n"
        
        # Add note for new wallets
        new_wallets = [r for r in results if r.get("is_new_wallet") and r.get("status") == "success"]
        if new_wallets:
            result_message += "⚠️ New wallets were created for some recipients.\n"
            result_message += "They will receive wallet details when they interact with the bot.\n\n"
    
    # Clear batch payment data
    if is_multi:
        context.user_data.pop("batch_payment_multi", None)
    else:
        context.user_data.pop("batch_payment", None)
    
    await query.edit_message_text(
        result_message,
        reply_markup=back_to_menu_keyboard(),
        disable_web_page_preview=True,
        parse_mode='Markdown'
    )

async def process_scheduled_payments(context: ContextTypes.DEFAULT_TYPE = None) -> None:
    """Process all due scheduled payments."""
    logger.info("Running scheduled payments check")
    
    # Get all due scheduled payments
    due_payments = get_all_due_scheduled_payments()
    
    if not due_payments:
        logger.info("No scheduled payments due")
        return
        
    logger.info(f"Found {len(due_payments)} scheduled payments to process")
    
    for payment in due_payments:
        try:
            # Get sender wallet
            sender_id = payment["sender_id"]
            wallet = get_wallet(sender_id)
            
            if not wallet:
                logger.error(f"Wallet not found for sender {sender_id}, skipping payment")
                continue
                
            from_address = wallet["address"]
            private_key = wallet["private_key"]
            
            to_address = payment["recipient_address"]
            amount_wei = payment["amount_wei"]
            display_name = payment["recipient_display"]
            
            # Check sender's balance
            balance_wei = w3.eth.get_balance(from_address)
            
            # Calculate gas
            gas_params = calculate_optimal_gas()
            
            # For legacy transactions
            if 'gasPrice' in gas_params:
                gas_price = gas_params['gasPrice']
                estimated_gas_cost_wei = gas_price * gas_params['gasLimit']
            else:  # For EIP-1559 transactions
                gas_price = gas_params['maxFeePerGas']
                estimated_gas_cost_wei = gas_params['maxFeePerGas'] * gas_params['gasLimit']
            
            # Check if balance can cover amount + gas
            if balance_wei < (amount_wei + estimated_gas_cost_wei):
                logger.error(f"Insufficient balance for scheduled payment: {sender_id} to {display_name}")
                continue
            
            # Prepare transaction
            if 'gasPrice' in gas_params:
                # Legacy transaction
                tx = {
                    'nonce': w3.eth.get_transaction_count(from_address),
                    'to': to_address,
                    'value': amount_wei,
                    'gas': gas_params['gasLimit'],
                    'gasPrice': gas_params['gasPrice'],
                    'chainId': 1  # Mainnet
                }
            else:
                # EIP-1559 transaction
                tx = {
                    'nonce': w3.eth.get_transaction_count(from_address),
                    'to': to_address,
                    'value': amount_wei,
                    'gas': gas_params['gasLimit'],
                    'maxFeePerGas': gas_params['maxFeePerGas'],
                    'maxPriorityFeePerGas': gas_params['maxPriorityFeePerGas'],
                    'chainId': 1,  # Mainnet
                    'type': 2  # EIP-1559
                }
            
            # Sign and send transaction
            signed_tx = w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            logger.info(f"Scheduled payment sent: {tx_hash.hex()}")
            
            # Send notification to recipient if it's a username
            if display_name.startswith('@'):
                username = display_name.lstrip('@')
                notification = {
                    "_id": str(datetime.now().timestamp()),
                    "type": "received_eth",
                    "amount": payment["amount"],
                    "sender_username": "Scheduled Payment",
                    "tx_hash": tx_hash.hex(),
                    "timestamp": datetime.now().isoformat(),
                    "new_wallet": payment.get("is_new_wallet", False)
                }
                
                # Add to pending notifications
                save_pending_notification(username, notification)
            
            # Update next execution time or mark as complete
            if payment["schedule_type"] == "one-time":
                # One-time payment is complete
                update_scheduled_payment(payment["_id"], {"active": False})
            else:
                # Calculate next execution time
                if payment["schedule_type"] == "weekly":
                    # Use croniter to calculate next occurrence
                    cron = croniter(payment["schedule_value"], datetime.now())
                    next_execution = cron.get_next(datetime)
                elif payment["schedule_type"] == "periodic":
                    # Add days to current time
                    next_execution = datetime.now() + timedelta(days=payment["schedule_value"])
                else:
                    # Unknown schedule type
                    logger.error(f"Unknown schedule type: {payment['schedule_type']}")
                    continue
                
                # Update the payment record
                update_scheduled_payment(payment["_id"], {"next_execution": next_execution})
                
            # Notify the sender via Telegram if context is available
            if context and isinstance(context, ContextTypes.DEFAULT_TYPE) and context.bot:
                try:
                    await context.bot.send_message(
                        chat_id=sender_id,
                        text=f"✅ Scheduled payment sent!\n\n"
                            f"To: {display_name}\n"
                            f"Amount: {payment['amount']} ETH\n"
                            f"TX: https://etherscan.io/tx/{tx_hash.hex()}",
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    logger.error(f"Failed to send notification to sender: {e}")
                
        except Exception as e:
            logger.error(f"Error processing scheduled payment: {e}")



# Add this after your other utility functions, before the command handlers
async def retry_telegram_action(action_func, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await action_func()
        except (telegram.error.TimedOut, telegram.error.NetworkError) as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Telegram action failed, retrying ({attempt+1}/{max_retries}): {e}")
            await asyncio.sleep(1)


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token with increased timeout
    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .connection_pool_size(4)  # Reduced connection pool size
        .read_timeout(30)         # Read timeout in seconds
        .write_timeout(30)        # Write timeout in seconds
        .connect_timeout(30)      # Connection timeout in seconds
        .build()
    )
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("create", create_wallet))
    application.add_handler(CommandHandler("import", import_wallet))
    application.add_handler(CommandHandler("balance", check_balance))
    application.add_handler(CommandHandler("address", get_address))
    application.add_handler(CommandHandler("pay", pay))
    application.add_handler(CommandHandler("price", check_eth_price))
    application.add_handler(CommandHandler("schedule", schedule_payment))
    application.add_handler(CommandHandler("scheduled", manage_scheduled_payments))
    application.add_handler(CommandHandler("batchpay", batch_payment))
    application.add_handler(CommandHandler("batchpaymulti", batch_payment_multi))
    
    # Add specific callback query handlers first (pattern matching)
    application.add_handler(CallbackQueryHandler(confirm_payment, pattern="^confirm_payment_"))
    application.add_handler(CallbackQueryHandler(process_batch_transaction, pattern="^confirm_batch_"))
    application.add_handler(CallbackQueryHandler(button_handler))
        
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Schedule job to process scheduled payments
    job_queue = application.job_queue
    job_queue.run_repeating(process_scheduled_payments, interval=60, first=10)
    
    # Log startup information
    logger.info("Starting Ethereum Wallet Bot with polling...")
    
    try:
        # Start the Bot strictly in polling mode
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
    except Exception as e:
        logger.error(f"Error in bot startup: {e}")
    finally:
        logger.info("Bot is shutting down")

if __name__ == "__main__":
    main()