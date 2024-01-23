from sqlalchemy import inspect
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import time


def check_for_duplicates(conn):
    """ 
    Checking for duplicate rows in all tables of specified database connection.
    """
    for table in inspect(conn).get_table_names():
        unique_in_table = pd.read_sql("SELECT count(*) from (select DISTINCT * FROM {}) as temp".format(table), conn)
        total_in_table = pd.read_sql("SELECT count(*) from {}".format(table), conn)
        print("Table", table)
        print("Total rows:", total_in_table["count"][0])
        print("Unique rows:", unique_in_table["count"][0], "\n")
        
        
        
        
################################################################################################################
###########    sdn_metrics - average bytes by server (taking into account huge skewness of the data)

def bytes_by_server_look_all(conn):
    """ 
    Primary analysis of data (READS ALL TABLE) - not the best decision 
    """
    server_bytes = pd.read_sql("""
    SELECT server_id, agents_pair, pkts, bytes, connection_id
    FROM sdn_metrics
    ORDER by bytes
    """,conn)
    display(server_bytes.describe())
    print("Skewness of bytes:",server_bytes['bytes'].skew(),"\n")
    
    
def mark_anomaly(row,column,ul,ll):
    """ 
    Based on upper limit (ul) and lower limit (ll), returns if value of a row and column is a potential anomaly 
    row and column - defines value that will be checked agains ul and ll
    """
    if row[column] > ul or row[column] < ll:
        return 1
    return 0


def identify_anomaly_IQR(df, column):
    """ 
    IQR method to determine upper and lower limits to find potential anomalies for specified column 
    df - data set to look for anomalies in
    """
    df_final = df
    Q1=df[column].quantile(0.25)
    Q3=df[column].quantile(0.75)
    IQR=Q3-Q1
    ul = Q3+1.5*IQR
    ll = Q1-1.5*IQR    
    df_final['is_anomaly'] = df_final.apply(lambda row: mark_anomaly(row,column,ul,ll), axis=1)
    return df_final     
    
    
def agg_bytes_by_server_by_column(column,conn):
    """ 
    Get bytes average grouped by agents_pair
    Identify potential anomalies of those averages
    First look into data with potential anomalies removed
    column - which column to look into while taking average for each server
    """
    server_bytes = pd.read_sql("""
    SELECT avg(bytes), {}
    FROM sdn_metrics
    GROUP by agents_pair
    ORDER by avg
    """.format(column),conn)

    print("Describe bytes average grouped by",column)
    display(server_bytes.describe())
    sns.boxplot(x=server_bytes["avg"])
    plt.show()
    print("")

    identified_outliers = identify_anomaly_IQR(server_bytes, "avg")
    no_outliers = identified_outliers[identified_outliers['is_anomaly'] == 0]
    only_outliers = identified_outliers[identified_outliers['is_anomaly'] == 1]
    
    print("Data with 'big bytes' removed:")
    print("Skewness of bytes:",no_outliers['avg'].skew(),"\n")
    display(no_outliers.describe())
    sns.boxplot(x=no_outliers["avg"])
    plt.show()
    print("\nData of only 'big bytes':")
    display(only_outliers.describe())

    return identified_outliers


def look_into_big_bytes(df,conn):
    """ 
    Count total number of SDN_metrics records per each server
    What part of data might these potential anomaly bytes consist?
    df - data set with identified potential anomalies
    """
    count_by_agents = pd.read_sql("""
        SELECT count(*), agents_pair
        FROM sdn_metrics
        GROUP by agents_pair
        ORDER by count
        """, conn)

    total_rows_sdn_metrics = sum(count_by_agents["count"])
    print("Total number of rows in sdn_metrics table:", total_rows_sdn_metrics)

    no_anomalies = df[df['is_anomaly'] == 0]
    only_anomalies = df[df['is_anomaly'] == 1]

    big_agents = only_anomalies['agents_pair'].tolist()
    print("'Big' agents found:", len(big_agents),"(out of",len(count_by_agents),"total agents)")
    remaining_agents = count_by_agents[~count_by_agents['agents_pair'].isin(big_agents)]
    remaining_rows_sdn_metrics = sum(remaining_agents["count"])
    print("Number of rows in snd_metrics table without 'Big' agents:", remaining_rows_sdn_metrics)
    print("Would result in:", (total_rows_sdn_metrics - remaining_rows_sdn_metrics) / total_rows_sdn_metrics * 100,
          "% drop (", total_rows_sdn_metrics - remaining_rows_sdn_metrics, "rows ).")

    return big_agents



def agg_usual_bytes_and_big_bytes(df,conn):
    """ 
    Get average bytes by server (excluding records with agents_pairs that had incredibly much data) - usual records
    Get a ratio of server's total records vs server's big byte agent_pairs records
    df - list of agents that had records with incredibly big byte amounts on average
    """
    big_agents_tuple = tuple(df)
    print("Query 1/2 running...")
    print("Start:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    avg_server_bytes = pd.read_sql("""
        SELECT s.server_id id, AVG(sm.bytes) as avg_bytes 
        FROM servers s 
        LEFT JOIN sdn_metrics sm
            ON s.server_id = sm.server_id 
            AND sm.agents_pair NOT IN {}
        GROUP BY id
        """.format(big_agents_tuple), conn)
    print("End:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))

    avg_server_bytes["avg_bytes"] = avg_server_bytes["avg_bytes"].fillna(0)
    print("This part represents the 'usual-sized-connections' which are ~90% of all connections:")
    display(avg_server_bytes.describe(include='all'))
    print("Skewness of avg_bytes:", avg_server_bytes['avg_bytes'].skew(), "\n")
    print("")

    print("Added ratio of total_connections with 'big_connections':")
    print("Query 2/2 running...")
    big_agent_servers = pd.read_sql("""
        WITH total_count AS(
        SELECT s.server_id id, count(sm.server_id) as total_sdn_reports
        FROM servers s
        LEFT JOIN sdn_metrics sm
            ON  s.server_id = sm.server_id
        GROUP BY id
        ),
        avg_bytes AS(
        SELECT s.server_id id, count(agents_pair) as big_conn_reports
        FROM servers s 
        LEFT JOIN sdn_metrics sm
            ON s.server_id = sm.server_id 
            AND sm.agents_pair IN {}
        GROUP BY id
        ORDER BY big_conn_reports desc)

        SELECT tc.id, tc.total_sdn_reports, COALESCE(ab.big_conn_reports*1.0/NULLIF(tc.total_sdn_reports,0),0) big_agent_proc
        FROM total_count tc
        JOIN avg_bytes ab
            ON tc.id = ab.id
        """.format(big_agents_tuple), conn)

    display(big_agent_servers.describe())
    print("Number of servers that processed 'Big' agents:",
          len(big_agent_servers[big_agent_servers["big_agent_proc"] > 0]))
    print("Double check of 'Big' agent rows:",
          int((big_agent_servers["big_agent_proc"] * big_agent_servers["total_sdn_reports"]).sum()))

    return pd.merge(big_agent_servers,avg_server_bytes, on='id')
        
        
        
        
################################################################################################################
###########    peer_metrics - SDN interfaces assigned by server

def agg_assigned_SDN_intf_by_server(conn):
    """ 
    For each server count the number of assigned specific SDN1, SDN2, SDN3 interfaces
    Count the total of all SDN interfaces assigned to each server
    """
    print("Query start:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    assigned_SDNs = pd.read_sql(""" 
        WITH tSDN1_COUNT AS (
        SELECT count(pm.sdn1_path) SDN1_COUNT, s.server_id
        FROM servers s
        LEFT join peer_metrics pm
            ON pm.sdn1_path::int = s.server_id
        GROUP by s.server_id),

        tSDN2_COUNT AS (
        SELECT count(pm.sdn2_path) SDN2_COUNT, s.server_id
        FROM servers s
        LEFT join peer_metrics pm
            ON pm.sdn2_path::int = s.server_id
        GROUP by s.server_id),

        tSDN3_COUNT AS (
        SELECT count(pm.sdn3_path) SDN3_COUNT, s.server_id
        FROM servers s
        LEFT join peer_metrics pm
            ON pm.sdn3_path::int = s.server_id
        GROUP by s.server_id),

        seperate_counts AS(
        SELECT s.server_id id, tSDN1_COUNT.SDN1_COUNT sdn1_intf_c, tSDN2_COUNT.SDN2_COUNT sdn2_intf_c, tSDN3_COUNT.SDN3_COUNT sdn3_intf_c
        FROM servers s
        JOIN tSDN1_COUNT ON s.server_id = tSDN1_COUNT.server_id
        JOIN tSDN2_COUNT ON s.server_id = tSDN2_COUNT.server_id
        JOIN tSDN3_COUNT ON s.server_id = tSDN3_COUNT.server_id)

        SELECT *, COALESCE(sdn1_intf_c,0) + COALESCE(sdn2_intf_c,0) + COALESCE(sdn3_intf_c,0) total_sdn_int_count
        FROM seperate_counts 
        ORDER BY total_sdn_int_count desc
        """, conn)
    print("Query end:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    display(assigned_SDNs.describe(include='all'))
    print("Skewness of assigned_sdn_count:", assigned_SDNs['total_sdn_int_count'].skew(), "\n")
    print("Number of servers that had NO SDN interface assigned at all:",
          len(assigned_SDNs[assigned_SDNs["total_sdn_int_count"] == 0]))
    return assigned_SDNs
        
        
        
        
################################################################################################################
###########    peer_metrics - packet losses by server

def check_packet_loss_exceptions(conn):
    """ 
    Check if there are any exceptions, because packet loss can be only between 0 and 1, otherwise it might disrupt the statistic
    """
    pl_stats_exceptions = pd.read_sql("""
        SELECT *
        FROM peer_metrics
        WHERE sdn1_packet_loss NOT between 0 and 1
        OR sdn2_packet_loss NOT between 0 and 1
        OR sdn3_packet_loss NOT between 0 and 1
        """, conn)

    display(pl_stats_exceptions.head(n=10))
    print("Rows that are very strange:",len(pl_stats_exceptions))
    print("Packet losses:")
    display(set(pl_stats_exceptions[pl_stats_exceptions["sdn1_packet_loss"] > 1]["sdn1_packet_loss"]))
    display(set(pl_stats_exceptions[pl_stats_exceptions["sdn2_packet_loss"] > 1]["sdn2_packet_loss"]))
    display(set(pl_stats_exceptions[pl_stats_exceptions["sdn3_packet_loss"] > 1]["sdn3_packet_loss"]))


def agg_packet_loss_stats(conn):
    """ 
    Get the average of packet loss for each SDN for each server and get the common average
    """
    print("Query start:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    packet_loss_avg = pd.read_sql("""
        WITH tSDN1_AVG AS (
        SELECT avg(pm.sdn1_packet_loss) sdn1_pl_avg, s.server_id
        FROM servers s
        LEFT JOIN peer_metrics pm
            ON s.server_id = pm.sdn1_path::int 
            AND sdn1_packet_loss BETWEEN 0 AND 1
        GROUP BY server_id
        ORDER BY sdn1_pl_avg),

        tSDN2_AVG AS (
        SELECT avg(pm.sdn2_packet_loss) sdn2_pl_avg, s.server_id
        FROM servers s
        LEFT JOIN peer_metrics pm
            ON s.server_id = pm.sdn2_path::int 
            AND sdn2_packet_loss BETWEEN 0 AND 1
        GROUP BY server_id
        ORDER BY sdn2_pl_avg),

        tSDN3_AVG AS (
        SELECT avg(pm.sdn3_packet_loss) sdn3_pl_avg, s.server_id
        FROM servers s
        LEFT JOIN peer_metrics pm
            ON s.server_id = pm.sdn3_path::int 
            AND sdn3_packet_loss BETWEEN 0 AND 1
        GROUP BY server_id
        ORDER BY sdn3_pl_avg)

        SELECT s.server_id id, tSDN1_AVG.sdn1_pl_avg, tSDN2_AVG.sdn2_pl_avg, tSDN3_AVG.sdn3_pl_avg
        FROM servers s
        JOIN tSDN1_AVG ON s.server_id = tSDN1_AVG.server_id
        JOIN tSDN2_AVG ON s.server_id = tSDN2_AVG.server_id
        JOIN tSDN3_AVG ON s.server_id = tSDN3_AVG.server_id
        ORDER BY sdn1_pl_avg desc
    """, conn)
    print("Query end:", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))

    packet_loss_avg['all_packet_loss_avg'] = packet_loss_avg.iloc[:, [1, 2, 3]].mean(axis=1)

    packet_loss_avg["sdn1_pl_avg"] = packet_loss_avg["sdn1_pl_avg"].fillna(1)
    packet_loss_avg["sdn2_pl_avg"] = packet_loss_avg["sdn2_pl_avg"].fillna(1)
    packet_loss_avg["sdn3_pl_avg"] = packet_loss_avg["sdn3_pl_avg"].fillna(1)
    packet_loss_avg["all_packet_loss_avg"] = packet_loss_avg["all_packet_loss_avg"].fillna(1)

    display(packet_loss_avg.describe())
    return packet_loss_avg


def color_negative_red(val):
    """ 
    Helping function to mark high correlated values in a table
    """
    color = 'red' if val > 0.6 or val < -0.6 else 'black'
    return 'color: % s' % color