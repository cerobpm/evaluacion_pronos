import json
import requests, psycopg2
import pandas as pd
from datetime import timedelta,datetime

with open("config.json") as f:
    config = json.load(f)

conn_string = "dbname='" + config["database"]["dbname"] + "' user='" + config["database"]["user"] + "' host='" + config["database"]["host"] + "' port='" + str(config["database"]["port"]) + "'"
try:
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
except:
    print( "No se ha podido establecer conexion.")
    exit(1)

def getCountPronos():
    # GET N pronos paraná
    n_pronos_parana = pd.read_sql_query("select unid as estacion_id,count(fecha_hoy),min(fecha_hoy),max(fecha_hoy) from tabprono_parana_historia group by unid order by unid", conn)
    n_pronos_parana["source"] = "tabprono_parana_historia"
    #print(n_pronos_parana)
    # GET N pronos paraguay
    n_pronos_paraguay = pd.read_sql_query("with pronos as (select distinct fecha_informe as f from informe_paraguay_prono_diario), unids as (select unid as estacion_id from estaciones where unid in (55,57,153,155)) select estacion_id,count(f),min(f),max(f) from pronos,unids group by estacion_id order by estacion_id", conn)
    n_pronos_paraguay["source"] = "informe_paraguay_prono_diario"
    #print(n_pronos_paraguay)
    # JOIN
    return pd.concat([n_pronos_parana,n_pronos_paraguay])

def getPronos(estacion_id, timestart=None, timeend=None):
    n_pronos = getCountPronos()
    if (estacion_id not in set(n_pronos["estacion_id"])):
        raise("No se encontró estacion_id en pronos")
    n_prono = n_pronos[n_pronos["estacion_id"]==estacion_id].to_dict("records")[0]
    pronos = None
    if(n_prono["source"]=="tabprono_parana_historia"):
        if(timestart is not None):
            if(timeend is not None):
                params = (estacion_id,timestart,timeend,estacion_id,timestart,timeend)
                return pd.read_sql_query('''with prono as (select fecha_hoy as forecast_date,fecha_pronostico as date, altura_pronostico as value from tabprono_parana_historia where unid=%s and fecha_hoy BETWEEN %s AND %s), tendencia as (select fecha_hoy as forecast_date,fecha_tendencia as date, altura_tendencia as value from tabprono_parana_historia where unid=%s and fecha_hoy BETWEEN %s AND %s) select * from prono union all select * from tendencia order by forecast_date,date;''', conn, params=params)
            else:
                params = (estacion_id,timestart,estacion_id,timestart)
                return pd.read_sql_query('''with prono as (select fecha_hoy as forecast_date,fecha_pronostico as date, altura_pronostico as value from tabprono_parana_historia where unid=%s and fecha_hoy >= %s), tendencia as (select fecha_hoy as forecast_date,fecha_tendencia as date, altura_tendencia as value from tabprono_parana_historia where unid=%s and fecha_hoy >= %s) select * from prono union all select * from tendencia order by forecast_date,date;''', conn, params=params)
        else:
            if(timeend is not None):
                params = (estacion_id,timeend,estacion_id,timeend)
                return pd.read_sql_query('''with prono as (select fecha_hoy as forecast_date,fecha_pronostico as date, altura_pronostico as value from tabprono_parana_historia where unid=%s and fecha_hoy <= %s), tendencia as (select fecha_hoy as forecast_date,fecha_tendencia as date, altura_tendencia as value from tabprono_parana_historia where unid=%s and fecha_hoy <= %s) select * from prono union all select * from tendencia order by forecast_date,date;''', conn, params=params)
            else:
                params = (estacion_id,estacion_id)
                return pd.read_sql_query('''with prono as (select fecha_hoy as forecast_date,fecha_pronostico as date, altura_pronostico as value from tabprono_parana_historia where unid=%s), tendencia as (select fecha_hoy as forecast_date,fecha_tendencia as date, altura_tendencia as value from tabprono_parana_historia where unid=%s) select * from prono union all select * from tendencia order by forecast_date,date;''', conn, params=params)
    elif(n_prono["source"]=="informe_paraguay_prono_diario"):
        estacion_id_column_map = {
            55: "pilc",
            57: "form",
            153: "bneg",
            155: "conc"
        }
        if(estacion_id not in estacion_id_column_map):
            raise("El estacion_id no corresponde con una estación de informe paraguay")
        value_column = estacion_id_column_map[estacion_id]
        select_stmt = f'select fecha_informe as forecast_date, fecha as date, {value_column} as value from informe_paraguay_prono_diario'
        if(timestart is not None):
            if(timeend is not None):
                params = (timestart,timeend)
                return pd.read_sql_query(select_stmt + ''' WHERE fecha_informe BETWEEN %s AND %s order by fecha_informe,fecha;''', conn, params=params)
            else:
                params = (timestart,)
                return pd.read_sql_query(select_stmt + ''' WHERE fecha_informe >= %s order by fecha_informe,fecha;''', conn, params=params)
        else:
            if(timeend is not None):
                params = (timeend,)
                return pd.read_sql_query(select_stmt + ''' WHERE fecha_informe <= %s order by fecha_informe,fecha;''', conn, params=params)
            else:
                return pd.read_sql_query(select_stmt + ''' order by fecha_informe,fecha;''', conn)

def getObs(series_id,timestart,timeend):
    response = requests.get(
        config["api"]["url"] + '/obs/puntual/observaciones',
        params={
            'series_id': series_id,
            'timestart': timestart,
            'timeend': timeend
        },
        headers={'Authorization': 'Bearer ' + config["api"]["token"]},
    )
    json_response = response.json()
    obs = pd.DataFrame.from_dict(json_response)
    obs = obs.rename(columns={'timestart':'date','valor':'value'})
    obs = obs[['date','value']]
    obs['date'] = pd.to_datetime(obs['date']).dt.round('min')            # Fecha a formato fecha -- CAMBIADO PARA QUE CORRA EN PYTHON 3.5
    obs['value'] = obs['value'].astype(float)
    # obs['date'] = pd.to_datetime(obs['date']).dt.round('min')            # Fecha a formato fecha
    # obs.set_index(obs['date'], inplace=True)
    # del obs['date']
    return obs

def getObsRegular(series_id,timestart,timeend,**kwargs):
    params = {
        'series_id': series_id,
        'timestart': timestart,
        'timeend': timeend
    }
    for key in kwargs:
        params[key] = kwargs[key]
    response = requests.get(
        config["api"]["url"] + '/obs/puntual/series/%i/regular' % series_id,
        params=params,
        headers={'Authorization': 'Bearer ' + config["api"]["token"]},
    )
    json_response = response.json()
    obs = pd.DataFrame.from_dict(json_response)
    obs = obs.rename(columns={'timestart':'date','valor':'value'})
    obs = obs[['date','value']]
    obs['date'] = pd.to_datetime(obs['date']).dt.round('min')            # Fecha a formato fecha -- CAMBIADO PARA QUE CORRA EN PYTHON 3.5
    obs['value'] = obs['value'].astype(float)
    # obs['date'] = pd.to_datetime(obs['date']).dt.round('min')            # Fecha a formato fecha
    # obs.set_index(obs['date'], inplace=True)
    # del obs['date']
    return obs

def getSeries(tipo,**kwargs): # estacion_id,var_id,proc_id,unit_id,fuentes_id
    url = None
    if(tipo == "puntual"):
        url = config["api"]["url"] + "/obs/puntual/series"
    elif(tipo == "areal"):
        url = config["api"]["url"] + "/obs/areal/series"
    elif(tipo == "raster"):
        url = config["api"]["url"] + "/obs/raster/series"
    else:
        raise("tipo incorrecto")
    response = requests.get(
        url,
        params=kwargs,
        headers={'Authorization': 'Bearer ' + config["api"]["token"]},
    )
    return response.json()

def getSeriesId(tipo,**kwargs):
    series = getSeries(tipo,**kwargs)
    return [i["id"] for i in series]

def getHObs(estacion_id,timestart,timeend):
    series_id = getSeriesId("puntual",estacion_id=estacion_id,var_id=2,proc_id=1)
    if(not len(series_id)):
        raise("No se encontró serie de H para el estacion_id %s" % estacion_id)
    series_id = series_id[0]
    return getObs(series_id,timestart,timeend)

def getHObsDailyMean(estacion_id,timestart,timeend,exclude_nulls=True):
    series_id = getSeriesId("puntual",estacion_id=estacion_id,var_id=2,proc_id=1)
    if(not len(series_id)):
        raise("No se encontró serie de H para el estacion_id %s" % estacion_id)
    series_id = series_id[0]
    obs = getObsRegular(series_id,timestart,timeend,agg_func="mean",inst="true")
    obs["date"] = [d.date() for d in obs["date"]]
    if(exclude_nulls):
        return obs[obs["value"].notnull()]
    return obs

def innerJoin(obs,prono):
    left = obs.copy()
    left.index = left["date"]
    del left["date"]
    right = prono.copy()
    right.index = right["date"]
    del right["date"]
    joined = left.join(right,how="inner",lsuffix="_obs",rsuffix="_prono")
    #joined["date"] = joined.index
    return joined.reset_index()

def getHObsAndProno(estacion_id,timestart,timeend):
    obs = getHObsDailyMean(estacion_id,timestart,timeend)
    prono = getPronos(estacion_id,timestart,timeend)
    return innerJoin(obs,prono)

def extractByLeadTime(data,lead_time): # lead_time: number of days
    return data[(data["date"] - data["forecast_date"] == timedelta(days=lead_time))]

def getStats(data,lead_time):
    data = extractByLeadTime(data,lead_time)
    count = len(data)
    # forecast_dates = set(data["forecast_date"])
    # forecast_count = len(forecast_dates)
    begin_date = min(data["forecast_date"])
    end_date = max(data["forecast_date"])
    # TODO results = indicadores de eficiencia, estadísticos de obs y prono, etc
    results = None
    return {
        "count": count,
        "begin_date": begin_date,
        "end_date": end_date,
        "results": results
    }

def getAndEvaluate(estacion_id,lead_time,timestart=None,timeend=None): # lead_time: number of days
    data = getHObsAndProno(estacion_id,timestart,timeend)
    stats = getStats(data,lead_time)
    return stats
    

# EXAMPLE
estacion_id = 26
timestart = "2019-01-01"
timeend = "2022-04-22"
#lead_time = 5
data = getHObsAndProno(estacion_id,timestart,timeend)
data["lead_time"] = data["date"] - data["forecast_date"]
lt = set(data["lead_time"])
lead_times = [l.days for l in lt]
lead_times.sort()
results = {}
for lead_time in lead_times:
    # print(lead_time)
    stats = getStats(data,lead_time)
    results[lead_time] = stats
# TEST
# estacion_id = 34
# timestart = datetime.fromisoformat("2021-01-01")
# timeend = datetime.fromisoformat("2022-01-01")
# pronos = getPronos(estacion_id,timestart,timeend)
# pronos = getPronos(estacion_id,timestart,None)
# pronos = getPronos(estacion_id,None,timeend)
# estacion_id = 57
# pronos = getPronos(estacion_id,timestart,timeend)
# pronos = getPronos(estacion_id,timestart,None)
# pronos = getPronos(estacion_id,None,timeend)

# series_id = getSeriesId("puntual",estacion_id=55,proc_id=1,var_id=2)
# obs = getHObs(55,datetime.fromisoformat("2021-01-01"),datetime.fromisoformat("2022-01-01"))
# obs_regular = getObsRegular(55,"2021-01-01","2022-01-01",agg_func="mean",inst="true")
# HobsDay = getHObsDailyMean(55,"2021-01-01","2022-01-01")
# joined = innerJoin(HobsDay,pronos)
# obsAndProno = getHObsAndProno(55,"2019-01-01","2022-04-22")
