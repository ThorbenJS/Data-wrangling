## A kind of ETL / Data wrangling course project ##

This was a project aimed at learning how to use Apache Airflow and learning more about Python and the Pandas library in particular, more about Linux and more about PostgreSQL.

The concept here was to extract data which was basically downloading historical weather data through Kaggle API.

Then I transformed the data which meant cleaning it, doing some aggregations and other methods of transformation, creating two separate and new DataFrames or tables out of the original dataset. One with daily aggregations and the other with monthly aggregations.

Before the final laoding step I added a simple data validation task to make sure that the transformed DataFrames didn't include nonsensical values.

Finally the data was loaded into PostgreSQL into two tables of a database using Apache Airflow.

The actual data transformation Python logic with Pandas and Numpy I created in Jupyter Notebook and then just implemented the code into the actual Ariflwo DAG file.
So essentially the whole data "pipeline" including extraction, transformation, validation and loading happens in the DAG python/airflow file as tasks and methods, which is not a so to say real world way of doing things. But I did learn a lot about all the tools and tech I used in this basic project even though it doesn't exactly mirror a real world situation.
