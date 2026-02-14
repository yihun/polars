import polars as pl


def mutate_at(df, exclude_cols, calculations):
    """
    calculations: dict of {stuffix: function}
    """

    new_cols = []
    for col in df.columns:
        if col not in exclude_cols:
            for suffix, calc_fun in calculations.items():
                new_cols.append(calc_fun(pl.col(col)).alias(f"{col}_{suffix}"))
    return df.with_columns(new_cols)


# usage

df = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5, 6],
        "group": ["A", "A", "B", "B", "C", "C"],
        "age": [25, 30, 22, 40, 35, 28],
        "income": [50000, 60000, 45000, 80000, 72000, 52000],
        "status": ["active", "inactive", "active", "active", "inactive", "active"],
        "prefix_score1": [10, 20, 30, 40, 50, 60],
        "prefix_score2": [5, 15, 25, 35, 45, 55],
    }
)

df_with_features = mutate_at(
    df=df,
    exclude_cols=["id", "status", "group"],
    calculations={
        "pord2": lambda x: x * 2,
        "log": lambda x: x.log(),
        "scaled": lambda x: (x - x.mean()) / x.std(),
    },
)
