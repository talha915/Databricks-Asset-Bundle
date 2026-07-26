import os

from openai import OpenAI

from framework.config import (
    AI_GATEWAY_URL,
    AI_MODEL, 
    token
)



def get_client():

    # token = os.environ.get(
    #     "DATABRICKS_TOKEN"
    # )


    # if not token:

    #     raise Exception(
    #         "DATABRICKS_TOKEN not found"
    #     )

    client = OpenAI(

        api_key=token,

        base_url=AI_GATEWAY_URL
    )


    return client



def call_llm(prompt):


    client = get_client()


    response = client.chat.completions.create(

        model="dev.bronze.test",


        messages=[

            {
                "role": "system",

                "content":
                """
                You are an enterprise data quality
                analyst.

                Analyze data issues,
                explain possible root causes,
                and recommend actions.
                """
            },


            {
                "role": "user",

                "content": prompt
            }

        ],


        temperature=0
    )


    return (
        response
        .choices[0]
        .message
        .content
    )