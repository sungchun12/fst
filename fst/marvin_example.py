from marvin import ai_fn

@ai_fn
def explain_sql(sql_query: str) -> list[str]:
    """
    Given sql query text, returns an explanation of the SQL and how performant it is in a plain string. Also, what could be better about the SQL in terms of performance?
    """


test = explain_sql("select * from test limit 5655")


print(test)


@ai_fn
def explain_sql_performance(sql_query: str) -> list[str]:
    """
    Given sql query text, returns an explanation of what could be better about the SQL in terms of performance and formatting
    """


test_perf = explain_sql_performance("select * from test limit 5655")
print(test_perf)