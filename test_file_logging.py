from datetime import timedelta, date, datetime, time

f = open("test.log", "a")


f.write(f'Starting uploading table: - {datetime.now()}')
f.flush()

print("Sending message in Slack")
f.flush()

f.write(f'Ending uploading table: - {datetime.now()}')
f.flush()