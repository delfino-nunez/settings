from datetime import datetime, timedelta
from collections import defaultdict


def generate_backup_timeline(backups, csv_output="backup_timeline.csv"):
    """
    Generate a timeline for backups taken, print to console, and save to a CSV file.

    :param backups: List of backups
    :param csv_output: Name of the CSV output file
    """
    # Parse the backup events and sort them by DB name
    parsed_events = sorted(
        [
            {
                "date": datetime.strptime(event["date"], "%Y-%m-%d"),
                "db": event["db"],
                "size": event["size"]
            }
            for event in backups
        ],
        key=lambda x: x["db"],
    )

    # Find the overall timeline range
    min_date = min(event["date"] for event in parsed_events)
    max_date = max(event["date"] for event in parsed_events)
    max_db_len = max(len(event["db"]) for event in parsed_events) + 1

    # Group days by months
    current_date = min_date
    timeline_days = []
    months = defaultdict(list)
    while current_date <= max_date:
        timeline_days.append(current_date)
        months[current_date.strftime("%B")].append(current_date)
        current_date += timedelta(days=1)

    # Prepare data for CSV
    csv_data = []

    # Console and CSV: Print the centered header (months)
    header_row = ["Timeline:".ljust(max_db_len)]
    print(header_row[0], end="")
    for month, days in months.items():
        month_label = month.center(len(days) * 3, "-")
        print(month_label, end="|")
    print()

    # Console and CSV: Print days row
    days_row = ["".ljust(max_db_len)]
    print(days_row[0], end="")
    for day in timeline_days:
        print(day.strftime("%d").ljust(3), end="")
        days_row.append(day.strftime("%b-%d"))
    print()
    csv_data.append(days_row)

    # ANSI color codes
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    # Console and CSV: Add each database's timeline
    databases = {event["db"] for event in parsed_events}
    for db in sorted(databases):
        row = [db.ljust(max_db_len)]
        print(row[0], end="")
        for current_date in timeline_days:
            if any(event["date"] == current_date and event["db"] == db for event in parsed_events):
                # print(f"\u2713".ljust(3), end="")
                print(f" {GREEN}\u2713{RESET} ", end="")
                row.append("YES")
            else:
                print(f" {RED}-{RESET} ", end="")
                # print("\u2717".ljust(3), end="")
                row.append("NO")
        print()
        csv_data.append(row)

    # Save to CSV file
    # with open(csv_output, "w") as csv_file:
    #     for row in csv_data:
    #         csv_file.write(",".join(row) + "\n")
    # print(f"Timeline saved to {csv_output}")


# Example usage
files = [
    {"date": "2023-01-01", "db": "Proj A", "size": "1024"},
    {"date": "2023-01-06", "db": "Project B", "size": "1024"},
    {"date": "2023-01-08", "db": "P C", "size": "1024"},
    {"date": "2023-01-02", "db": "Proj A", "size": "1024"},
    {"date": "2023-01-02", "db": "Project B", "size": "1024"},
    {"date": "2023-01-02", "db": "P C", "size": "1024"},
    {"date": "2023-01-05", "db": "Proj A", "size": "1024"},
    {"date": "2023-01-07", "db": "Proj A", "size": "1024"},
    {"date": "2023-01-09", "db": "Proj A", "size": "1024"},
    {"date": "2023-01-10", "db": "Proj A", "size": "1024"},
    {"date": "2023-02-01", "db": "Project B", "size": "1024"},
    {"date": "2023-03-31", "db": "P C", "size": "1024"},
    {"date": "2023-03-01", "db": "Project B", "size": "1024"},
    {"date": "2023-02-10", "db": "P C", "size": "1024"},
]

generate_backup_timeline(files)
