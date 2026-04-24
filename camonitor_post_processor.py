import pandas as pd
import matplotlib.pyplot as plt

# Input
RAW_FILE_NAME = 'rocking_11564_Cr_0255(in).csv'

RAW_COLUMNS = ["signal", "date", "hour", "measure"]
RAW_HEADER = None
RAW_SEP = r"\s+" 

X_VAR = "DCM"
Y_VAR = 'QUA:A:PICO03:Current4'

TIME_UNIT_ROUNDING = (3, 2, 1, 0)

PROC_FILE_PATH = "processed/"
PROC_DECIMAL = "."

if __name__ == "__main__":
    df = pd.read_csv(RAW_FILE_NAME, sep=RAW_SEP, header=RAW_HEADER, names=RAW_COLUMNS)

    # Verificar se foi tudo no mesmo dia
    if len(df.date.unique()) == 0:
        df.rename(columns={"hour": "time"}, inplace=True)
    else:
        df["time"] = df['date'] + ' ' + df['hour']
        df.drop("hour", axis=1, inplace=True)

    df.drop("date", axis=1, inplace=True)

    # Transformar o tempo em intervalo desde o início da coleta (em segundos)
    df.time = pd.to_datetime(df.time)
    df.time = df.time-df.time.min()
    df.time = df.time.dt.total_seconds()

    # Ordenar pelo tempo
    df.sort_values(by="time", axis=0, inplace=True, ascending=True, ignore_index=True)

    # Verificar quantidade de sinais diferentes e tamanho de cada um
    all_sources = {}
    x_source = ""
    for source in df.signal.unique():
        all_sources[source] = len(df[df.signal==source])
        if X_VAR in source:
            x_source = source

    # Separar todos os pares de interesse (em função da variável x)
    all_pairs = {
        source: (x_source, source) for source in all_sources.keys() if source != x_source  
    }


    # Vamos começar com um par arbitrario, sendo 'QUA:A:PICO03:Current3' a variável y
    y_source = Y_VAR

    # Separar dataframes do par escolhido
    x_df = df[df.signal == x_source].drop("signal", axis=1)
    y_df = df[df.signal == y_source].drop("signal", axis=1)

    # Testar diferentes arredondamentos do tempo
    time_x, time_y = x_df.time, y_df.time
    aux, final_d = 1000, 1000
    for i, n_decimals in enumerate(TIME_UNIT_ROUNDING):
        if i == 0:
            aux = abs(len(time_x.round(n_decimals).unique()) - len(time_y.round(n_decimals).unique()))
        else:
            if aux <= abs(len(time_x.round(n_decimals).unique()) - len(time_y.round(n_decimals).unique())):
                final_d = n_decimals

    # Arrendondar de fato
    x_df.time = x_df.time.round(final_d)
    y_df.time = y_df.time.round(final_d)
    # Se tiver zero casas decimais de precisão, podemos transformar em inteiro!
    if n_decimals == 0:
        x_df.time = x_df.time.astype(int)
        y_df.time = y_df.time.astype(int)

    # Remover duplicatas (mesmo sinal, mesmo instante e mesma medida)
    x_df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
    y_df.drop_duplicates(keep="first", inplace=True, ignore_index=True)

    # Substitur medidas de instantes repetidos pela média delas
    x_df = x_df.groupby("time").mean()
    y_df = y_df.groupby("time").mean()


    # Verificar tamanho das séries após transformações e ajustar tempo de início e fim
    if len(x_df) > len(y_df):
        if x_df.index[0] < y_df.index[0]:
            x_df = x_df.iloc[list(x_df.index).index(y_df.index[0]):]
        if x_df.index[-1] > y_df.index[-1]:
            x_df = x_df.iloc[:list(x_df.index).index(y_df.index[-1])]


    if len(y_df) > len(x_df):
        if y_df.index[0] < x_df.index[0]:
            y_df = y_df.iloc[list(y_df.index).index(x_df.index[0]):]
        if y_df.index[-1] > x_df.index[-1]:
            y_df = y_df.iloc[:list(y_df.index).index(x_df.index[-1])]

    print(f"{y_df.index=}, {len(y_df.index)=}")
    print("\n")
    print(f"{x_df.index=}, {len(x_df.index)=}")

    # Juntar as duas séries em um dataframe
    df_final = pd.concat([x_df, y_df], axis=1, ignore_index=True, names=[x_source, y_source])
    df_final.columns = [x_source, y_source]
    if df_final.index[0] != 0:
        df_final.set_index(df_final.index-df_final.index[0], inplace=True)

    # Gerar gráfico da dispersão
    plt.plot(df_final[x_source], df_final[y_source], ".")
    plt.title(f"{x_source} X {y_source}")
    plt.savefig(f"{PROC_FILE_PATH}/{x_source.rsplit(":",1)[-1]}_{y_source.rsplit(":",1)[-1]}_{RAW_FILE_NAME.rsplit(".",1)[0]}.png")

    # Salvar arquivo csv dos dados sincronizados
    df_final.to_csv(f"{PROC_FILE_PATH}/{x_source.rsplit(":",1)[-1]}_{y_source.rsplit(":",1)[-1]}_{RAW_FILE_NAME}", decimal=PROC_DECIMAL)

