import pandas as pd
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
import time
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA


def KMeans_draw_elbow_for_n_clusters(df,clusters):
    """ 
    Draws the 'elbow' graph, which helps to determine the number of clusters
    Where an 'elbow' can be seen in graph - that is the potential number of clusters
    df - data to cluster
    clusters - max number of clusters
    """
    scaler = preprocessing.MinMaxScaler()
    data_scaled = scaler.fit_transform(df)
    inertia = []
    K = range(1, 8)
    for k in K:
        print('Processing', k, 'cluster:', time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        kmeanModel = KMeans(n_clusters=k).fit(df)
        kmeanModel.fit(df)
        inertia.append(kmeanModel.inertia_)

    # Plot the elbow
    plt.plot(K, inertia, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Inertia')
    plt.show()


def KMeans_clustering(n_clusters,df,full_data):
    """ 
    Clustering using KMeans method
    n_clusters - selected number of clusters
    df - data to cluster
    full_data - data to append cluster column to
    """
    scaler = preprocessing.MinMaxScaler()
    data_scaled = scaler.fit_transform(df)
    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(data_scaled)
    labels = pd.DataFrame(kmeans.labels_)
    labeled_df = pd.concat((full_data, labels), axis=1)
    labeled_df = labeled_df.rename({0: 'cluster'}, axis=1)
    print("Potentially 'good' servers:", len(labeled_df[labeled_df["cluster"] == 0]))
    print("Potentially 'bad' servers:", len(labeled_df[labeled_df["cluster"] == 1]))
    return labeled_df


def PCA_clustering(n_clusters,df):
    """ 
    Clustering data with PCA
    n_clusters - number of clusters to format
    df - data to cluster
    """
    Sc = StandardScaler()
    X = Sc.fit_transform(df)
    print('Fitting PCA start:', time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    pca = PCA(n_clusters)
    pca_data = pd.DataFrame(pca.fit_transform(X), columns=['PC1', 'PC2'])
    print('Clustering start:', time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    kmeans = KMeans(n_clusters=n_clusters).fit(X)
    pca_data['pca_cluster'] = pd.Categorical(kmeans.labels_)
    print('Done!', time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    return pca_data


def compare_and_merge_results(pca_df, kmeans_df):
    """ 
    Merge results of Kmeans and pca clustering
    Determine on which servers these methods 'agree'
    Mark the 'grey area' servers
    pca_df - pca clustering results
    kmeans_df - kmeans clustering results
    """
    kmeans_df = kmeans_df.rename({'cluster': 'knn_cluster'}, axis=1)

    print("(PCA) Potentially 'good' servers:", len(pca_df[pca_df["pca_cluster"] == 0]))
    print("(PCA) Potentially 'bad' servers:", len(pca_df[pca_df["pca_cluster"] == 1]))
    print(" ")
    print("(KNN) Potentially 'good' servers:", len(kmeans_df[kmeans_df["knn_cluster"] == 0]))
    print("(KNN) Potentially 'bad' servers:", len(kmeans_df[kmeans_df["knn_cluster"] == 1]))

    clustered2_servers = pd.concat((kmeans_df, pca_df), axis=1)

    def f(x):
        if x['knn_cluster'] == x['pca_cluster']:
            return x['pca_cluster']
        else:
            return 3

    print(" ")
    clustered2_servers['global_cluster'] = clustered2_servers.apply(f, axis=1)
    print("Amount of servers assigned 'grey area' category 3:",
          len(clustered2_servers[clustered2_servers['global_cluster'] == 3]))
    clustered2_servers['global_cluster'] = clustered2_servers['global_cluster'].astype(int)

    return clustered2_servers