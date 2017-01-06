from database import MonitorDatabase

if __name__ == '__main__':
    with MonitorDatabase('/app/data/monitor.db') as db:
        for row in db.fetch_and_parse_latest():
            print row
