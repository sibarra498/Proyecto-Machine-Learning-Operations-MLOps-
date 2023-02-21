from fastapi import FastAPI, Form
import pandas as pd

app = FastAPI()

@app.get('/')
def root():
	return "Hello, listo pa probar esta poderosa api?"
	#return {"msg": "Hello, listo pa probar esta poderosa api?"}


def acoplar_db():
	bd0 = pd.read_csv("bases_datos.csv", index_col="id")
	Dict_Users = {
		1 : "1",
		2 : "2",
		3 : "3",
		4 : "4",
		5 : "5",
		6 : "6",
		7 : "7",
		8 : "8"
	}
	return bd0, Dict_Users

def UnificarDbUsers(Dict_Users):
	Db_Users = pd.DataFrame()
	for i in Dict_Users.values():		
		#print(i)
		Db_Users = pd.concat([Db_Users, pd.read_csv(i + ".csv")])
	return Db_Users


def MeterScores(bd0, Db_Users):
	Db_Scores = Db_Users.drop(["userId", "timestamp"], axis=1).groupby(['movieId']).mean()["rating"]
	Db_Scores = Db_Scores.rename("Score")
	bd = pd.concat([bd0, Db_Scores], axis = 1)
	return bd


def tratamiento_db():
	bd0, Dict_Users = acoplar_db()
	Db_Users = UnificarDbUsers(Dict_Users)
	bd = MeterScores(bd0, Db_Users)
	print("\n\ndb lista¡\n\n")
	return bd

bd = tratamiento_db()

@app.post("/get_max_duration")
def get_max_duration(platform: str, year: int, duration_type: str):
	db_b = bd[bd.Plataforma == platform]
	db_b2 = db_b[db_b.release_year == year]
	db_b3 = db_b2[db_b2.duration_type == duration_type]
	db_b4 = db_b3.sort_values("duration_int", ascending=False)
	db5 = db_b4[db_b4.duration_int == db_b4.iloc[0].duration_int]
	pelicula = db5.iloc[0].title
	pelicula1 = db5.iloc[0].duration_int	
	return {
		"plataforma": platform,
		"pelicula mayor duración": pelicula,
		"duración": pelicula1,
		"duration_type": duration_type
	}

@app.post('/score_count')
def score_count(platform: str, year: int, score: int):
	db_b = bd[bd.Plataforma == platform]
	db_b1 = db_b[db_b.release_year == year]
	db_b2 = db_b1[db_b1.Score > score]
	Numero = len(db_b2)	
	return {
		"plataforma": platform,
		"# de ocurrencias ScoreP": Numero
	}



@app.post("/get_count_platform")
def get_count_platform(platform : str):
	db = bd[bd.Plataforma == platform]
	db1 = bd[bd.type == "movie"]
	Numero = len(db1)	
	return {
		"plataforma": platform,
		"# de peliculas": Numero
	}
	

# + Actor que más se repite según plataforma y año.
@app.post("/get_actor")
def get_actor(platform: str, year: int):	
	# Se separan los actores en diferentes columnas y luego se unifican en una sola
	# para saber cual es el actor que mas se repite	
	Df_Plataforma = bd[bd.Plataforma.isin([platform])&bd.release_year.isin([year])]	
	Expand = Df_Plataforma["cast"].str.split(",", expand=True)	
	Unificado = pd.DataFrame()
	for i in Expand.columns:
		Unificado = pd.concat([Unificado, Expand[i]])	
	Moda = Unificado.mode()[0][0]
	#print(f"Actor más repetido: {Moda}\n")
	return {
		"plataforma": platform,
		"year": year,
		"Actor más repetido": Moda
	}
