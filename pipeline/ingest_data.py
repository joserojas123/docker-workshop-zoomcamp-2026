import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg_user', default='root', help='Usuario de la base de datos MySQL/ClickHouse')
@click.option('--pg_pass', default='root', help='Contraseña de la base de datos MySQL/ClickHouse')
@click.option('--pg_host', default='localhost', help='Host de PostgreSQL')
@click.option('--pg_port', default=5432, help='Puerto de PostgreSQL', type=int)
@click.option('--pg_db', default='ny_taxi', help='Nombre de la base de datos PostgreSQL')
@click.option('--year', default=2021, help='Año de los datos', type=int)
@click.option('--month', default=1, help='Mes de los datos', type=int)
@click.option('--target_table', default='yellow_taxi_data', help='Tabla destino')
@click.option('--chunksize', default=100000, help='Tamaño del chunk para procesamiento', type=int)

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize):



    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    print(url)

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))


if __name__ == '__main__':
    run()