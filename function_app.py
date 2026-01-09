import os                           #reads secret values like DB password, server name etc..
import json
import logging                      #write logs that are visible in Azure portal
import datetime
import requests                     #calls the api
import pyodbc                       #talks to sql database
import azure.functions as func

#creating an app
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

STATIONS = [                #later we loop through these stations
    "Brugge",
    "Brussels-Central",
    "Gent-Sint-Pieters",
    "Antwerpen-Centraal"]

# Helper: Database connection

def get_db_connection():
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={os.getenv('DB_SERVER')};"
        f"Database={os.getenv('DB_NAME')};"
        f"Uid={os.getenv('DB_USER')};"
        f"Pwd={os.getenv('DB_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)    #returning a live connection


# Fetch & store iRail data

def fetch_and_store_departures(station: str) -> int:
    url = "https://api.irail.be/liveboard/"
    params = {
        "station": station,
        "format": "json",
        "lang": "en"}

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()

    data = response.json()
    departures = data.get("departures", {}).get("departure", [])

    conn = get_db_connection()       #opening database
    cursor = conn.cursor()

    inserted = 0

    for d in departures:            #looping over each train
        try:
            train_id = d.get("vehicle", "").replace("BE.NMBS.", "")
            platform = d.get("platform")
            delay = int(d.get("delay", 0))
            departure_time = datetime.datetime.fromtimestamp(int(d.get("time")))

            cursor.execute(             #insert into database
                """
                INSERT INTO departures (
                    station,
                    train_id,
                    departure_time,
                    delay_seconds,
                    platform,
                    created_at)
                VALUES (?, ?, ?, ?, ?, GETDATE())
                """,
                station,
                train_id,
                departure_time,
                delay,
                platform)

            inserted += 1

        except pyodbc.IntegrityError:
            # duplicate ignored due to unique index
            continue

    conn.commit()
    cursor.close()
    conn.close()

    return inserted


# HTTP Trigger - manual use

@app.route(route="fetch_irail_data", methods=["GET"])        #creates an endpoint
def fetch_irail_data(req: func.HttpRequest) -> func.HttpResponse:
    station = req.params.get("station", "Brugge")            #if no station given, Brugge is default

    try:
        inserted = fetch_and_store_departures(station)
        return func.HttpResponse(
            json.dumps({
                "station": station,
                "inserted_rows": inserted
            }),
            mimetype="application/json",
            status_code=200)
    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(str(e), status_code=500)


# Timer Trigger — every 5 minutes

@app.timer_trigger(
    schedule="0 */5 * * * *",  # every 5 minutes
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True)
def fetch_irail_data_timer(timer: func.TimerRequest) -> None:
    logging.info("Timer trigger started (every 5 minutes)")

    for station in STATIONS:
        try:
            inserted = fetch_and_store_departures(station)

            logging.info(
                f"Timer success | station={station} | inserted={inserted}")

        except Exception as e:
            logging.error(
                f"Timer failed | station={station} | error={str(e)}")
