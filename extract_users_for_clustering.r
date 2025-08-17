library(cluster)
library(factoextra)
library(dplyr)

# データ準備
df <- dataset
features <- c('total_page_views', 'unique_pages', 'avg_engagement_time', 'product_page_ratio')
df_clean <- df[complete.cases(df[features]), ]

# データ標準化
df_scaled <- scale(df_clean[features])

# K-means クラスタリング
set.seed(123)
k_means_result <- kmeans(df_scaled, centers = 5, nstart = 25)

# 結果の追加
df_clean$cluster <- as.factor(k_means_result$cluster)

# 可視化
fviz_cluster(k_means_result, data = df_scaled,
             palette = c("#2E9FDF", "#00AFBB", "#E7B800", "#FC4E07", "#8B008B"),
             geom = "point",
             ellipse.type = "convex", 
             ggtheme = theme_bw())

# PowerBIに結果を返す
dataset <- df_clean
