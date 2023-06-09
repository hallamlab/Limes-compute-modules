if __name__ == "__main__":
    import os, sys
    import pandas as pd
    import sqlite3
    
    tsv, raw_columns, ref, out_annots, out_groups = sys.argv[1:]

    columns = raw_columns.split(";")
    df_an = pd.read_csv(tsv, sep="\t", header=None)
    df_an.columns = columns
    df_an.to_csv(tsv.replace(".tsv", ".csv"), index=False) # overwrites

    PROT_COLS = ['name', 'bigg_reaction', 'gos', 'pfam', 'pname', 'ogs', 'orthoindex', 'kegg_ko', 'kegg_cog', 'kegg_disease', 'kegg_ec', 'kegg_brite', 'kegg_rclass', 'kegg_tc', 'kegg_cazy', 'kegg_pathway', 'kegg_module', 'kegg_reaction', 'kegg_go', 'kegg_drug', 'kegg_pubmed', 'kegg_network']
    GROUP_COLS = ['og', 'level', 'nm', 'description', 'COG_categories']

    with sqlite3.connect(ref) as con:
        cur = con.cursor()
        def _get_prot(k):
            return next(cur.execute(f"select * from prots where name='{k}'"))
        
        def _get_grp(k, l):
            return next(cur.execute(f"select * from og where og='{k}' and level='{l}'"))
        
        gi = PROT_COLS.index('ogs')
        prots = []
        group_ks = set()
        for _, row in df_an.iterrows():
            key = row.stitle
            annot = _get_prot(key)
            _groups = annot[gi] if annot[gi] is not None else ""
            group_ks.update([tuple(g.split("@")) for g in _groups.split(",") if "COG" not in g])
            prots.append(annot)
        dfp = pd.DataFrame(prots, columns=PROT_COLS)
        dfp.to_csv(out_annots, index=False)

        groups = []
        for gk, level in group_ks:
            groups.append(_get_grp(gk, level))
        dfg = pd.DataFrame(groups, columns=GROUP_COLS)
        dfg.to_csv(out_groups, index=False)
