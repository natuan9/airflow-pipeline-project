import requests
import logging
from airflow.models import Variable

def send_telegram_alert(context):
    """
    Send alert to Telegram when a task fails.
    
    Usage in DAG:
        default_args = {
            'on_failure_callback': send_telegram_alert,
        }
    
    Required Airflow Variables:
        - TELEGRAM_BOT_TOKEN: Your bot token from @BotFather
        - TELEGRAM_CHAT_ID: Your group/channel chat ID
    """
    try:
        # Get Telegram credentials from Airflow Variables
        bot_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
        chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var=None)
        
        if not bot_token or not chat_id:
            logging.warning("Telegram credentials not configured. Skipping alert.")
            return
        
        # Extract task information from context
        task_instance = context.get('task_instance')
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context.get('execution_date')
        exception = context.get('exception')
        log_url = task_instance.log_url
        
        # Build alert message
        message = f"""
🚨 *Airflow Task Failed!*

*DAG:* `{dag_id}`
*Task:* `{task_id}`
*Execution Date:* `{execution_date}`
*Error:* `{str(exception)[:200]}`

[View Logs]({log_url})
"""
        
        # Send to Telegram
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            logging.info(f"Telegram alert sent successfully for {dag_id}.{task_id}")
        else:
            logging.error(f"Failed to send Telegram alert: {response.text}")
            
    except Exception as e:
        logging.error(f"Error sending Telegram alert: {e}")
