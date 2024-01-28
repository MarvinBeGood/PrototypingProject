import polars as pl

pl.Config.set_fmt_str_lengths(1000)


def main():
    df_hunt = pl.read_csv(
        "hunt_showdown_herausforderung.csv",separator=";"
    )
    df_event_points_only = df_hunt.filter(
        pl.col("belohnungs_währung") == pl.lit("Eventpunkte")
    )
    df_event_points_only = df_event_points_only.with_row_index("row_number", 1)

    df_event_points_only = df_event_points_only.with_columns(
        (pl.col("belohnung_value").sum()).alias("Eventpunkte_summe_pro_Woche")
    )
    df_event_points_only = df_event_points_only.with_columns(
        (pl.col("belohnung_value").cum_sum()).alias(
            "Eventpunkte_max_summe_bis_herausforderung_punkte"
        )
    )

    with pl.Config(fmt_str_lengths=50, tbl_rows=1000):
        print(df_event_points_only)


if __name__ == "__main__":
    main()
