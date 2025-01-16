"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd
    import zipfile
    import os

    input_folder = "files/input/"
    output_folder = "files/output/"

    os.makedirs(output_folder, exist_ok=True)
    client_df = pd.DataFrame(
        columns=[
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    )
    campaign_df = pd.DataFrame(
        columns=[
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_day",
        ]
    )
    economics_df = pd.DataFrame(
        columns=["client_id", "cons_price_idx", "euribor_three_months"]
    )

    def process_client_data(df):
        df["job"] = df["job"].str.replace(".", "").str.replace("-", "_")
        df["education"] = (
            df["education"].str.replace(".", "_").replace("unknown", pd.NA)
        )
        df["credit_default"] = df["credit_default"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        df["mortgage"] = df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
        return df[
            [
                "client_id",
                "age",
                "job",
                "marital",
                "education",
                "credit_default",
                "mortgage",
            ]
        ]

    def process_campaign_data(df):
        df["previous_outcome"] = df["previous_outcome"].apply(
            lambda x: 1 if x == "success" else 0
        )
        df["campaign_outcome"] = df["campaign_outcome"].apply(
            lambda x: 1 if x == "yes" else 0
        )
        df["last_contact_day"] = pd.to_datetime(
            df["day"].astype(str) + "-" + df["month"] + "-2022", format="%d-%b-%Y"
        )
        return df[
            [
                "client_id",
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "last_contact_day",
            ]
        ]

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_folder, file_name), "r") as zip_ref:
                for file in zip_ref.namelist():
                    if file.endswith(".csv"):
                        with zip_ref.open(file) as f:
                            df = pd.read_csv(f)
                            client_df = pd.concat(
                                [client_df, process_client_data(df)], ignore_index=True
                            )
                            campaign_df = pd.concat(
                                [campaign_df, process_campaign_data(df)],
                                ignore_index=True,
                            )
                            economics_df = pd.concat(
                                [
                                    economics_df,
                                    df[
                                        [
                                            "client_id",
                                            "cons_price_idx",
                                            "euribor_three_months",
                                        ]
                                    ],
                                ],
                                ignore_index=True,
                            )

    campaign_df.rename(columns={"last_contact_day": "last_contact_date"}, inplace=True)

    client_df.to_csv(os.path.join(output_folder, "client.csv"), index=False)
    campaign_df.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    economics_df.to_csv(os.path.join(output_folder, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()
