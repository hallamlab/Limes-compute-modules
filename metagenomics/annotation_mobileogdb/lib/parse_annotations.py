if __name__ == "__main__":

    import os, sys
    import pandas as pd

    tsv, raw_columns, ref, out = sys.argv[1:]
    columns = raw_columns.split(";")

    df_an = pd.read_csv(tsv, sep="\t", header=None)
    df_an.columns = columns
    df_an.stitle = [s.split("|")[0] for s in df_an.stitle]
    hits = set(df_an["stitle"].unique())
    df_an.to_csv(tsv.replace(".tsv", ".csv"), index=False) # overwrites

    df_ref = pd.read_csv(ref)
    df_hits = df_ref[df_ref["mobileOG Entry Name"].isin(hits)]
    df_hits = df_hits[[c for c in df_hits.columns if c != "Amino Acid Sequence"]]
    df_hits.to_csv(out, index=False)
