from pyspark.sql.session import SparkSession
from pyspark.sql.functions import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import row_number, rank
import logging
import sys
class Analysis_crash_data:
    def __init__(self):
        print("init")

    def reading_data(self,path, spark) -> DataFrame:
        logging.info("Reading data from",path)
        df = spark.read.option("header", "True").option("inferSchema", "True").csv(path)
        return df
    def write_data(self,path,df) -> DataFrame:
        logging.info("Writing data at",path)
        df.coalesce(2).write.parquet(path)


    def analysis_function(self,inputLocation,OutputLocation,spark,logger):
        '''
        :param inputLocation:
        :param OutputLocation:
        :param master:
        '''

        dir_path = inputLocation
        output_path = OutputLocation


        logger.info("Initializing parameters")
        path_charges = dir_path + "/" + "Charges_use.csv"
        path_Damages = dir_path + "/" + "Damages_use.csv"
        path_Endorse = dir_path + "/" + "Endorse_use.csv"
        path_Primary_Person = dir_path + "/" + "Primary_Person_use.csv"
        path_Restrict = dir_path + "/" + "Restrict_use.csv"
        path_units = dir_path + "/" + "Units_use.csv"
        '''
        Reading Data commencing
        '''
        df_charges = self.reading_data(path_charges, spark)
        df_Damages = self.reading_data(path_Damages, spark)
        df_Endorse = self.reading_data(path_Endorse, spark)
        df_Primary_Person = self.reading_data(path_Primary_Person, spark)
        df_Restrict = self.reading_data(path_Restrict, spark)
        df_units = self.reading_data(path_units, spark)

        '''
        1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        '''
        df_male_death_Count = df_Primary_Person.filter(df_Primary_Person.PRSN_INJRY_SEV_ID != "NOT INJURED").filter(
            df_Primary_Person.PRSN_GNDR_ID == "MALE").filter(df_Primary_Person.DEATH_CNT > 0)
        death_count = df_male_death_Count.count()
        logger.info("Death male count: ")

        logger.info(death_count)

        '''
        2.	Analysis 2: How many two wheelers are booked for crashes? 
        '''
        df_two_wheeler = df_units.filter("UNIT_DESC_ID == 'MOTOR VEHICLE' OR UNIT_DESC_ID == 'PEDALCYCLIST'")

        logger.info("Two wheeler crashed: ")
        two_wheeler_count = df_two_wheeler.count()
        logger.info(two_wheeler_count)

        '''
        3.	Analysis 3: Which state has highest number of accidents in which females are involved? 
        '''
        df_units = df_units.withColumnRenamed("UNIT_NBR", "Number_of_deaths").withColumnRenamed("TOT_INJRY_CNT",
                                                                                                "Total_injured")
        df_join_person_unit = df_units.join(df_Primary_Person, df_units.CRASH_ID == df_Primary_Person.CRASH_ID)
        df_join_person_unit_next = df_join_person_unit
        df_join_person_unit_ethbds = df_join_person_unit
        df_join_person_unit_last = df_join_person_unit

        df_join_person_unit = df_join_person_unit.filter(df_join_person_unit.PRSN_GNDR_ID == "FEMALE").groupby(
            df_join_person_unit.VEH_LIC_STATE_ID).max("Number_of_deaths").withColumnRenamed("max(Number_of_deaths)",
                                                                                            "Max_deaths").orderBy(
            sf.col("Max_deaths").desc())

        df_join_person_unit = df_join_person_unit.limit(10)
        write_analysis3 = output_path+"/"+"Analysis3"
        logger.info("Analysis3 result")
        df_join_person_unit.show(10,False)

        self.write_data(write_analysis3,df_join_person_unit)

        '''
        4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        '''

        # w1 = Window.partitionBy(df_join_person_unit_next.VEH_MAKE_ID).orderBy(df_join_person_unit_next.Total_injured)
        df_join_person_unit_next = df_join_person_unit_next.groupby(df_join_person_unit_next.VEH_MAKE_ID).max(
            "Total_injured").withColumnRenamed("max(Total_injured)", "Highest_injured").orderBy(
            sf.col("Highest_injured").desc())
        w1 = Window.partitionBy(lit("a")).orderBy(lit("a"))
        fifth_fifteenth = df_join_person_unit_next.withColumn("row_number", row_number().over(w1))
        fifth_fifteenth = fifth_fifteenth.filter(sf.col("row_number").between(5, 15))
        logger.info("Analysis 4 result")
        fifth_fifteenth.show(10, False)
        write_analysis4 = output_path+"/"+"Analysis4"
        self.write_data(write_analysis4, fifth_fifteenth)
        '''
        5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        '''

        df_join_person_unit_ethbds = df_join_person_unit_ethbds.select(df_join_person_unit_ethbds.PRSN_ETHNICITY_ID,
                                                                       df_join_person_unit_ethbds.VEH_BODY_STYL_ID).groupby(
            df_join_person_unit_ethbds.VEH_BODY_STYL_ID, df_join_person_unit_ethbds.PRSN_ETHNICITY_ID).count()
        # df_join_person_unit_ethbds = df_join_person_unit_ethbds.groupby(df_join_person_unit_ethbds.VEH_BODY_STYL_ID).agg(max("count"))
        w2 = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(sf.col("count").desc())
        df_join_person_unit_ethbds = df_join_person_unit_ethbds.withColumn("Rank", rank().over(w2))
        df_join_person_unit_ethbds = df_join_person_unit_ethbds.filter(sf.col("Rank") == 1)
        write_analysis5 = output_path+"/"+"Analysis5"
        logger.info("Analysis5 result")
        df_join_person_unit_ethbds.show(1,False)

        self.write_data(write_analysis5,df_join_person_unit_ethbds)
        '''
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) 
        '''

        df_Primary_Person_other = df_Primary_Person
        df_Primary_Person_other = df_Primary_Person_other.filter(
            df_Primary_Person_other.PRSN_ALC_RSLT_ID == "Positive").filter(sf.col("DRVR_ZIP").isNotNull())

        df_Primary_Person_other = df_Primary_Person_other.groupby(df_Primary_Person_other.DRVR_ZIP).count().orderBy(
            sf.col("count").desc())
        df_Primary_Person_other = df_Primary_Person_other.limit(5)
        write_analysis6 = output_path+"/"+"Analysis6"
        #self.write_data(write_analysis6,df_Primary_Person_other)
        logger.info("Analysis 6 result")
        df_Primary_Person_other.show(5,False)

        '''
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance 
        '''

        df_join_unit_insdamage = df_units.filter(
            "VEH_DMAG_SCL_1_ID == 'DAMAGED 4' OR VEH_DMAG_SCL_2_ID == 'DAMAGED 4' AND FIN_RESP_TYPE_ID == 'PROOF OF LIABILITY INSURANCE'")

        df_no_property_damage = df_join_unit_insdamage.join(df_Damages,
                                                            df_join_unit_insdamage.CRASH_ID == df_Damages.CRASH_ID,
                                                            "leftanti")

        distinct_car_crash_ids = df_join_unit_insdamage.select(df_join_unit_insdamage.CRASH_ID).distinct().count()
        logger.info("Analysis 7 result")
        logger.info(distinct_car_crash_ids)

        '''
        8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data
        '''
        df_units_max_crash = df_join_person_unit_last.filter(df_join_person_unit_last.VEH_LIC_STATE_ID != "NA").filter(
            df_join_person_unit_last.DRVR_LIC_TYPE_ID != "UNLICENSED").groupby(
            df_join_person_unit_last.VEH_LIC_STATE_ID).count().orderBy(sf.col("count").desc())
        list_max_crash = df_units_max_crash.select(df_units_max_crash.VEH_LIC_STATE_ID).limit(25).collect()
        array_max_state_crash = [row.VEH_LIC_STATE_ID for row in list_max_crash]
        print(array_max_state_crash)
        df_top_colors = df_units.groupby(df_join_person_unit_last.VEH_COLOR_ID).count().orderBy(sf.col("count").desc())
        list_colors = df_top_colors.select(df_top_colors.VEH_COLOR_ID).limit(10).collect()
        list_top_colors = [row.VEH_COLOR_ID for row in list_colors]
        print(list_top_colors)
        df_join_person_unit_last = df_join_person_unit_last.filter(
            "CONTRIB_FACTR_1_ID == 'FAILED TO CONTROL SPEED' OR CONTRIB_FACTR_P1_ID == 'FAILED TO CONTROL SPEED' OR CONTRIB_FACTR_2_ID == 'FAILED TO CONTROL SPEED'")
        df_join_person_unit_last = df_join_person_unit_last.filter(
            df_join_person_unit_last.DRVR_LIC_TYPE_ID != "UNLICENSED").filter(
            df_join_person_unit_last.VEH_LIC_STATE_ID.isin(array_max_state_crash)).filter(
            df_join_person_unit_last.VEH_COLOR_ID.isin(list_top_colors))
        df_join_person_unit_last = df_join_person_unit_last.groupby(
            df_join_person_unit_last.VEH_MAKE_ID).count().orderBy(sf.col("count").desc())
        df_join_person_unit_last = df_join_person_unit_last.limit(5)
        logger.info("Analysis 8 result")
        df_join_person_unit_last.show(5,False)
        write_analysis8 = output_path+"/"+"Analysis8"
        self.write_data( write_analysis8,df_join_person_unit_last)
