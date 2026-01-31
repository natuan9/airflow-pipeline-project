# Telegram Alert Setup Guide

## Step 1: Create a Telegram Bot

1. Open Telegram and search for `@BotFather`
2. Send the command `/newbot`
3. Name your bot (e.g., `Airflow Monitor Bot`)
4. Set a username for the bot (must end with `bot`, e.g., `airflow_monitor_bot`)
5. **Save the TOKEN** provided by BotFather (format: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`)

## Step 2: Get the Chat ID

### Option 1: Create a Group Chat (Recommended)
1. Create a new Telegram Group
2. Add the bot to the group (search by its username)
3. Send any message in the group
4. Access the following URL (replace `YOUR_BOT_TOKEN` with your actual token):
   ```
   https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates
   ```
5. Find `"chat":{"id":-1234567890}` in the JSON response
6. **Save the Chat ID** (include the `-` sign if present)

### Option 2: Direct Chat with Bot
1. Find the bot by its username and start a chat
2. Send the message `/start`
3. Access the `getUpdates` URL as mentioned above
4. Get the Chat ID (a positive number, without the `-` sign)

## Step 3: Configure Airflow Variables

Run the following commands to save the credentials in Airflow:

```bash
# Replace YOUR_BOT_TOKEN and YOUR_CHAT_ID with actual values
docker exec airflow-pipeline-project-airflow-scheduler-1 \
  airflow variables set TELEGRAM_BOT_TOKEN "123456789:ABCdefGHIjklMNOpqrsTUVwxyz"

docker exec airflow-pipeline-project-airflow-scheduler-1 \
  airflow variables set TELEGRAM_CHAT_ID "-1234567890"
```

## Step 4: Test Alert

1. Access Airflow UI (http://localhost:8081)
2. Trigger any DAG
3. Or test by stopping Kafka/Spark to let the DAG fail
4. Check your Telegram group/chat for the alert

## Troubleshooting

### Not receiving alerts?
- Check if the bot has been added to the group
- Check if the bot has permission to send messages in the group
- Check Airflow logs: `docker compose logs airflow-scheduler | grep -i telegram`

### "Chat not found" error?
- Incorrect Chat ID or bot not added to the group
- Try sending a new message in the group and fetch the Chat ID again

### "Unauthorized" error?
- Incorrect bot token
- Re-verify the token from BotFather
