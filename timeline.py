from datetime import datetime, timedelta

def get_date_range(start_date, end_date):
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date)
        current_date += timedelta(days=1)
    return date_range

def print_timeline(events):
    # Sort events by start date
    events.sort(key=lambda x: datetime.strptime(x['start'], '%Y-%m-%d'))

    # Get overall start and end dates
    start_date = datetime.strptime(min(event['start'] for event in events), '%Y-%m-%d')
    end_date = datetime.strptime(max(event['end'] for event in events), '%Y-%m-%d')

    # Generate date range
    date_range = get_date_range(start_date, end_date)

    # Print header
    month_header = "    "
    day_header = "    "
    current_month = None
    month_start_index = 0

    for i, date in enumerate(date_range):
        if date.month != current_month:
            if current_month is not None:
                month_header += f"{date.strftime('%b'):^{(i - month_start_index) * 3}}"
            current_month = date.month
            month_start_index = i
        day_header += f"{date.day:2} "

    # Add the last month
    month_header += f"{date_range[-1].strftime('%b'):^{(len(date_range) - month_start_index) * 3}}"

    print(month_header)
    print(day_header)

    # Print events
    for event in events:
        event_start = datetime.strptime(event['start'], '%Y-%m-%d')
        event_end = datetime.strptime(event['end'], '%Y-%m-%d')

        # print(f"{event['text']}", end="")
        print("    ", end="")
        for date in date_range:
            if event_start <= date <= event_end:
                print(" # ", end="")
            else:
                print(" - ", end="")
        print(f" {event['text']}")

    # Print footer
    print("    " + "=" * (len(date_range) * 3))

# Example usage
events = [
    {"start": "2023-01-15", "end": "2023-02-28", "text": "Project A"},
    {"start": "2023-02-01", "end": "2023-03-15", "text": "Project B"},
    {"start": "2023-03-10", "end": "2023-04-20", "text": "Project C"},
]

print_timeline(events)
