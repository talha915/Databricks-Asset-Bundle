from framework.ai_client import call_llm
from framework.utils import get_spark

import pyspark.sql.functions as F


spark = get_spark()



def run_ai_quality_check(

    df,

    table_name,

    check_name,

    columns,

    prompt_template,

    limit_records=100

):


    rows = (

        df

        .select(
            *columns
        )

        .limit(
            limit_records
        )

        .collect()

    )


    results = []


    for row in rows:


        row_data = row.asDict()


        prompt = prompt_template.format(
            **row_data
        )


        try:

            ai_response = call_llm(
                prompt
            )

        except Exception as e:

            ai_response = (
                f"AI call failed: {str(e)}"
            )


        results.append(

            (

                table_name,

                check_name,

                str(row_data),

                ai_response

            )

        )


    if not results:

        return



    result_df = spark.createDataFrame(

        results,

        [

            "table_name",

            "check_name",

            "input_record",

            "ai_result"

        ]

    )



    (

        result_df

        .withColumn(

            "created_time",

            F.current_timestamp()

        )

        .write

        .format("delta")

        .mode("append")

        .saveAsTable(

            "ai_lab_demo.system.ai_quality_results"

        )

    )