import azure.functions as func
import logging
import pandas as pd
import numpy as np
import io
import time
from azure.storage.blob import BlobServiceClient, ResourceNotFoundError
from sqlalchemy import create_engine
import os
#import Metrica_IO as mio


app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="footballdatacontainer/{name}",
                               connection="storageaccountwrmag1_STORAGE") 

#IMPORT Z BLOB STORAGE

def main(myblob: func.InputStream):
    logging.info(f"Blob name: {myblob.name}, Size: {myblob.length} bytes")
    start_all = time.time() 

    blob_text = myblob.read().decode('utf-8')
    df = pd.read_csv(io.StringIO(blob_text))

    
    if "events.csv" in myblob.name:
        start_event = time.time() 
        logging.info("Processing events.csv...")
        events = df
        logging.info(f"Event df shape: {events.shape}")
        logging.info(f"Event df head:\n{events.head()}")
        end_event = time.time() 
        logging.info(f"Czas odczytu event.csv: {end_event - start_event} s")


    elif "tracking.csv" in myblob.name:
        start_track_total = time.time()
        logging.info("Processing tracking.csv...")

        # Retry tylko dla tracking.csv
        max_wait_seconds = 90
        wait_interval = 5
        elapsed = 0

        while True:
            try:
                start_track = time.time()
                # Próba wczytania blobu
                blob_text = myblob.read().decode('utf-8')
                tracking = pd.read_csv(io.StringIO(blob_text))
                end_track = time.time()
                break  # jeśli się udało, wychodzimy
            except Exception as e:
                if elapsed >= max_wait_seconds:
                    raise Exception(f"Nie udało się wczytać tracking.csv w ciągu {max_wait_seconds} sekund. Błąd: {e}")
                logging.info(f"Niezaładowano jeszcze plików trackingu, czekam {wait_interval}s (elapsed: {elapsed}s)")
                time.sleep(wait_interval)
                elapsed += wait_interval
        
        end_track_total = time.time()

        logging.info(f"Czas odczytu tracking.csv (włącznie z odczekiwaniem): {end_track_total - start_track_total}s")
        logging.info(f"Czas odczytu tracking.csv (nie licząc odczekiwania): {end_track - start_track}s")
        logging.info(f"Tracking df shape: {tracking.shape}")
        logging.info(f"Tracking df head:\n{tracking.head()}")

    else:
        logging.warning(f"Unknown file received: {myblob.name}")
# PRZETWARZANIE DANYCH I LICZENIE STATYSTYK
    end = time.time() 

    def to_metric_coordinates(data,field_dimen=(106.,68.) ):
        '''
        Convert positions from Metrica units to meters (with origin at centre circle)
        '''
        x_columns = [c for c in data.columns if c[-1].lower()=='x']
        y_columns = [c for c in data.columns if c[-1].lower()=='y']
        data = data.copy()
        data[x_columns] = ( data[x_columns]-0.5 ) * field_dimen[0]
        data[y_columns] = -1 * ( data[y_columns]-0.5 ) * field_dimen[1]
        return data

    homeaway = 'Home'

    # --------------------------------------------------------------------------------------------------

    text_cols = ['Team', 'Type', 'Subtype', 'From', 'To']
    for c in text_cols:
        events[c] = events[c].astype('category')
        
    # --------------------------------------------------------------------------------------------------

    events = to_metric_coordinates(events)
    tracking = to_metric_coordinates(tracking)

    # --------------------------------------------------------------------------------------------------

    team_events = events[events['Team'] == homeaway]
    team_events

    # --------------------------------------------------------------------------------------------------

    player_id = 10
    player = f"Player{player_id}"

    # --------------- PASS STATS ---------------

    succ_passes = team_events[team_events["Type"] == "PASS"] # Udane podania

    fail_pass_subtypes = [
        "INTERCEPTION",
        "HEAD-INTERCEPTION",
        "THEFT",
        "CROSS-INTERCEPTION",
        "GOAL KICK-INTERCEPTION",
        "DEEP BALL"
    ] # Te podtypy zdarzeń dla BALL LOST wiążą się z nieudanymi podaniami

    fail_passes = team_events[
        (team_events["Type"] == "BALL LOST") &
        (team_events["Subtype"].isin(fail_pass_subtypes))
    ] # Nieudane podania

    passes = pd.concat([succ_passes, fail_passes])
    passes_pl = passes[passes['From'] == player]

    # --------------------------------------------------------------------------------------------------

    #Drużynowe statystyki
    total_passes = len(passes) # Liczba podań
    succs_passes = len(passes[passes['Type'] == 'PASS']) # Liczba udanych podań
    pass_accuracy = (succs_passes / total_passes) * 100 if total_passes else 0 # Procent udanych podań
    pass_len = (np.sqrt((passes["End X"] - passes["Start X"])**2 + (passes["End Y"] - passes["Start Y"])**2)) # Odległości między podaniami
    mean_pass_len = pass_len.mean() # Średnia odległość podań
    max_pass_len = pass_len.max() # Najdłuższe podanie
    progressive = (passes["End X"] > passes["Start X"]).sum() # Liczba podań do przodu
    #Statystyki danego zawodnika
    pl_total_passes = len(passes_pl) # Liczba podań
    pl_succs_passes = len(passes_pl[passes_pl['Type'] == 'PASS']) # Liczba udanych podań
    pl_pass_accuracy = (pl_succs_passes / pl_total_passes) * 100 if pl_total_passes else 0 # Procent udanych podań
    pl_pass_len = np.sqrt((passes_pl["End X"] - passes_pl["Start X"])**2 + (passes_pl["End Y"] - passes_pl["Start Y"])**2) # Odległości między podaniami
    pl_mean_pass_len = pl_pass_len.mean() # Średnia odległość podań
    pl_max_pass_len = pl_pass_len.max() # Najdłuższe podanie
    pl_progressive = (passes_pl["End X"] > passes_pl["Start X"]).sum() # Liczba podań do przodu

    passes_data = {
        "stats": ['total_passes', 'succs_passes', 'pass_accuracy_pct', 'mean_pass_len', 'max_pass_len', 'progressive'],
        f"player{player_id}_stats": [pl_total_passes, pl_succs_passes, pl_pass_accuracy, pl_mean_pass_len, pl_max_pass_len, pl_progressive],
        "team_stats": [total_passes, succs_passes, pass_accuracy, mean_pass_len, max_pass_len, progressive]
    }

    passes_df = pd.DataFrame(passes_data)
    passes_df_t = passes_df.transpose()
    passes_df_t.columns = passes_df_t.iloc[0]
    passes_df_t = passes_df_t.iloc[1:,:]

    int_cols = ['total_passes', 'succs_passes', 'progressive']

    for col in passes_df_t.columns:
        passes_df_t[col] = pd.to_numeric(passes_df_t[col], errors='coerce')

    for col in passes_df_t.columns:
        if col in int_cols:
            passes_df_t[col] = passes_df_t[col].astype('Int64')  # pozwala na NaN i int
        else:
            passes_df_t[col] = passes_df_t[col].round(2)

    passes_df_t = passes_df_t.reset_index(names='stats')

    # --------------- SHOT STATS ---------------

    shots = team_events[team_events["Type"] == "SHOT"]
    shots_pl = shots[shots['From'] == player]

    # --------------------------------------------------------------------------------------------------

    total_shots = len(shots) # Liczba strzałów
    on_target = len(shots[shots['Subtype'].str.startswith('ON TARGET')]) # Liczba celnych strzałów
    shot_accuracy = (on_target / total_shots) * 100 if total_shots else 0 # Celność strzałów
    goals = len(shots[shots['Subtype'].str.endswith('GOAL')]) # Liczba goli
    on_target_to_goals = (goals / on_target) * 100 if total_shots else 0 # Liczba celnych strzałów zamienionych na goli
    # Statystyki zawodnika
    pl_total_shots = len(shots_pl) # Liczba strzałów
    pl_on_target = len(shots_pl[shots_pl['Subtype'].str.startswith('ON TARGET')]) # Liczba celnych strzałów
    pl_shot_accuracy = (pl_on_target / pl_total_shots) * 100 if pl_total_shots else 0 # Celność strzałów
    pl_goals = len(shots_pl[shots_pl['Subtype'].str.endswith('GOAL')]) # Liczba goli
    pl_on_target_to_goals = (pl_goals / pl_on_target) * 100 if pl_total_shots else 0 # Liczba celnych strzałów zamienionych na goli

    shots_data = {
        "stats": ['total_shots', 'on_target', 'shot_accuracy_pct', 'goals', 'on_target_to_goals_pct'],
        f"player{player_id}_stats" : [pl_total_shots, pl_on_target, pl_shot_accuracy, pl_goals, pl_on_target_to_goals],
        "team_stats":[total_shots, on_target, shot_accuracy, goals, on_target_to_goals]
    }

    shots_df = pd.DataFrame(shots_data)
    shots_df_t = shots_df.transpose()
    shots_df_t.columns = shots_df_t.iloc[0]
    shots_df_t = shots_df_t.iloc[1:,:]

    int_cols = ['total_shots', 'on_target', 'goals']

    for col in shots_df_t.columns:
        shots_df_t[col] = pd.to_numeric(shots_df_t[col], errors='coerce')

    for col in shots_df_t.columns:
        if col in int_cols:
            shots_df_t[col] = shots_df_t[col].astype('Int64')  # pozwala na NaN i int
        else:
            shots_df_t[col] = shots_df_t[col].round(2)

    shots_df_t = shots_df_t.reset_index(names='stats')

    # --------------- DUEL STATS ---------------

    duels_types = ['BALL LOST', 'CHALLENGE', 'RECOVERY', 'BALL OUT']
    duels = team_events[(team_events["Type"].isin(duels_types))]
    duels_pl = duels[duels['From'] == player]

    # --------------------------------------------------------------------------------------------------

    balls_lost = len(duels[duels['Type'] == 'BALL LOST']) ## Liczba straconych piłek
    recoveries = len(duels[duels['Type'] == 'RECOVERY']) ## Liczba odzyskanych piłek
    interceptions = len(duels[(duels['Type'] == 'RECOVERY') & (duels['Subtype'] == 'INTERCEPTION')]) ## Liczba przechwyconych piłek
    aerials = len(duels[duels['Subtype'].str.contains('AERIAL', na=False)]) # Liczba pojedynków w powietrzu
    aerials_won = len(duels[duels['Subtype'] == 'AERIAL-WON']) # Liczba wygranych pojedynków w powietrzu
    grounds = len(duels[duels['Subtype'].str.contains('GROUND', na=False)]) # Liczba pojedynków na ziemi
    grounds_won = len(duels[duels['Subtype'] == 'GROUND-WON']) # Liczba wygranych pojedynków na ziemi
    tackles = len(duels[duels['Subtype'].str.contains('TACKLE', na=False)]) # Liczba pojedynków przy odbiorze piłki
    tackles_won = len(duels[duels['Subtype'] == 'TACKLE-WON']) # Liczba wygranych pojedynków przy odbiorze piłki
    challenges = aerials + grounds + tackles ## Liczba pojedynków ogółem
    challenges_won = aerials_won + grounds_won + tackles_won ## Liczba wygranych pojedynków ogółem
    challenges_rate = challenges_won / challenges * 100 ##
    # Statystyki zawodnika
    pl_balls_lost = len(duels_pl[duels_pl['Type'] == 'BALL LOST']) ## Liczba straconych piłek
    pl_recoveries = len(duels_pl[duels_pl['Type'] == 'RECOVERY']) ## Liczba odzyskanych piłek
    pl_interceptions = len(duels_pl[(duels_pl['Type'] == 'RECOVERY') & (duels_pl['Subtype'] == 'INTERCEPTION')]) ## Liczba przechwyconych piłek
    pl_aerials = len(duels_pl[duels_pl['Subtype'].str.contains('AERIAL', na=False)]) # Liczba pojedynków w powietrzu
    pl_aerials_won = len(duels_pl[duels_pl['Subtype'] == 'AERIAL-WON']) # Liczba wygranych pojedynków w powietrzu
    pl_grounds = len(duels_pl[duels_pl['Subtype'].str.contains('GROUND', na=False)]) # Liczba pojedynków na ziemi
    pl_grounds_won = len(duels_pl[duels_pl['Subtype'] == 'GROUND-WON']) # Liczba wygranych pojedynków na ziemi
    pl_tackles = len(duels_pl[duels_pl['Subtype'].str.contains('TACKLE', na=False)]) # Liczba pojedynków przy odbiorze piłki
    pl_tackles_won = len(duels_pl[duels_pl['Subtype'] == 'TACKLE-WON']) # Liczba wygranych pojedynków przy odbiorze piłki
    pl_challenges = pl_aerials + pl_grounds + pl_tackles ## Liczba pojedynków ogółem
    pl_challenges_won = pl_aerials_won + pl_grounds_won + pl_tackles_won ## Liczba wygranych pojedynków ogółem
    pl_challenges_rate = pl_challenges_won / pl_challenges * 100 ##

    duels_data = {
        "stats": ['balls_lost', 'recoveries', 'interceptions', 'aerials', 'aerials_won', 'grounds', 'grounds_won', 
                                    'tackles', 'tackles_won', 'challenges', 'challenges_won', 'challenges_rate_pct'],
        f"player{player_id}_stats" : [pl_balls_lost, pl_recoveries, pl_interceptions, pl_aerials, pl_aerials_won, pl_grounds, pl_grounds_won, 
                                    pl_tackles, pl_tackles_won, pl_challenges, pl_challenges_won, pl_challenges_rate],
        "team_stats": [balls_lost, recoveries, interceptions, aerials, aerials_won, grounds, grounds_won, 
                                    tackles, tackles_won, challenges, challenges_won, challenges_rate]
    }

    duels_df = pd.DataFrame(duels_data)
    duels_df_t = duels_df.transpose()
    duels_df_t.columns = duels_df_t.iloc[0]
    duels_df_t = duels_df_t.iloc[1:,:]
    duels_df_t.iloc[:,0:11] = duels_df_t.iloc[:,0:11].astype(int)
    duels_df_t['challenges_rate_pct'] = duels_df_t['challenges_rate_pct'].apply(lambda x: f'{x:.2f}')
    duels_df_t = duels_df_t.reset_index(names='stats')

    # --------------- OTHER STATS ---------------

    delta_t = 0.04

    dx = tracking[f"{homeaway}_{player_id}_x"].diff()
    dy = tracking[f"{homeaway}_{player_id}_y"].diff()

    distance = np.sqrt(dx**2 + dy**2)

    # --------------------------------------------------------------------------------------------------

    speed = distance / delta_t
    speed = np.where(speed > 12, np.nan, speed)
    speed = pd.Series(speed).interpolate().fillna(method='bfill').fillna(method='ffill')

    avg_speed = np.mean(speed)#speed.mean()
    max_speed = np.max(speed)#speed.max()
    total_distance = np.sum(distance) #distance.sum()

    sprint_threshold = 7  # [m/s]
    sprints = (speed > sprint_threshold).sum()
    sprint_time = sprints * delta_t

    activetime = (speed > 0.5).sum() * delta_t

    # --------------------------------------------------------------------------------------------------

    tracking_stats_data = {
        'stats': ['avg_speed_m_s', 'max_speed_m_s', 'total_distance_m', 'sprints', 'sprint_time_s', 'activetime_s'],
        f'player{player_id}_stats' : [avg_speed, max_speed, total_distance, sprints, sprint_time, activetime]
    }

    tracking_stats = pd.DataFrame(tracking_stats_data)
    tracking_stats_t = tracking_stats.transpose()
    tracking_stats_t.columns = tracking_stats_t.iloc[0]
    tracking_stats_t = tracking_stats_t.iloc[1:,:]
    tracking_stats_t['sprints'] = tracking_stats_t['sprints'].astype(int)
    tracking_stats_t[['avg_speed_m_s', 'max_speed_m_s', 'total_distance_m']] = tracking_stats_t[['avg_speed_m_s', 'max_speed_m_s', 'total_distance_m']].astype(float).round(2).astype(str)
    tracking_stats_t = tracking_stats_t.reset_index(names='stats')
    tracking_stats_t

    # --------------------------------------------------------------------------------------------------

    player_nrs = [col.split('_')[1] for col in tracking.columns if f'{homeaway}_' in col]
    player_nrs = list(set(player_nrs))

    team_stats = []

    for p in player_nrs:
        dx = tracking[f'{homeaway}_{p}_x'].diff()
        dy = tracking[f'{homeaway}_{p}_y'].diff()
        distance = np.sqrt(dx**2 + dy**2)#.fillna(0)
        speed = distance / delta_t
        speed = np.where(speed > 12, np.nan, speed)
        speed = pd.Series(speed).interpolate().fillna(method='bfill').fillna(method='ffill')
        sprints = (speed > sprint_threshold).sum()
        sprint_time = sprints * delta_t
        activetime = (speed > 0.5).sum() * delta_t
        team_stats.append({
            "player": p,
            "avg_speed": np.mean(speed),
            "max_speed": np.max(speed),
            "total_distance": np.sum(distance).astype(float).round(2),
            "sprints": np.sum(sprints),
            "sprint_time": np.mean(sprint_time),
            "activetime": np.mean(activetime)
        })

    df_team = pd.DataFrame(team_stats)

    # --------------------------------------------------------------------------------------------------

    summary = pd.DataFrame({
        'stats': ['team_stats'],
        'avg_speed_m_s': [df_team["avg_speed"].mean()],
        'max_speed_m_s': [df_team["max_speed"].max()],
        'total_distance_m': [df_team["total_distance"].sum()],
        'sprints': [df_team["sprints"].sum()],
        'sprint_time_s': [df_team["sprint_time"].mean()],
        'activetime_s': [df_team["activetime"].mean()]
    }).round(2)

    tracking_stats = pd.concat([tracking_stats_t, summary])

# EKSPORT DO SYNAPSE

    # Connection string do Synapse (dedicated SQL pool)
    conn_str = (
    "mssql+pyodbc://{user}:{password}"
    "@{server}:1433/{db}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    ).format(
        user=os.environ["SYNAPSE_USER"],
        password=os.environ["SYNAPSE_PASSWORD"],
        server=os.environ["SYNAPSE_SERVER"],
        db=os.environ["SYNAPSE_DB"]
    )
    engine = create_engine(conn_str)

    start = time.time()

    # Wstawienie danych do tabeli event
    events.to_sql("event_table", engine, if_exists="append", index=False)
    # Wstawienie danych do tabeli tracking
    tracking.to_sql("tracking_table", engine, if_exists="append", index=False)
    # Wstawienie danych do tabeli event
    passes_df_t.to_sql("passes_stats_table", engine, if_exists="append", index=False)
    # Wstawienie danych do tabeli tracking
    shots_df_t.to_sql("shots_stats_table", engine, if_exists="append", index=False)
    # Wstawienie danych do tabeli event
    duels_df_t.to_sql("duels_stats_table", engine, if_exists="append", index=False)
    # Wstawienie danych do tabeli tracking
    tracking_stats.to_sql("tracking_stats_table", engine, if_exists="append", index=False)

    end = time.time()
    print(f"Załadowano w: {end - start}s")

    end_all = time.time()
    print(f"Czas wykonywania się funkcji: {end_all - start_all}s")

@app.route(route="TestHttp", auth_level=func.AuthLevel.ANONYMOUS)
def TestHttp(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

        # def blob_trigger(myblob: func.InputStream):
#     logging.info(
#         f"Blob triggered: {myblob.name}, size={myblob.length} bytes"
#     )
#     logging.info(f'pandas version: {pd.__version__}')
#     logging.info(f'numpy version: {np.__version__}')

# This example uses SDK types to directly access the underlying BlobClient object provided by the Blob storage trigger.
# To use, uncomment the section below and add azurefunctions-extensions-bindings-blob to your requirements.txt file
# Ref: aka.ms/functions-sdk-blob-python
#
# import azurefunctions.extensions.bindings.blob as blob
# @app.blob_trigger(arg_name="client", path="mycontainer",
#                   connection="storageaccountwrmag1_STORAGE")
# def blob_trigger(client: blob.BlobClient):
#     logging.info(
#         f"Python blob trigger function processed blob \n"
#         f"Properties: {client.get_blob_properties()}\n"
#         f"Blob content head: {client.download_blob().read(size=1)}"
#     )