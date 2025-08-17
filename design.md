## モデル設計のポイント
* キー設計：user_pseudo_id と session_id を中心にリレーションを構築
* フィルター方向：Users → Sessions → Events の一方向フィルターが基本
* 集計効率：セッション時間やイベント数などは BigQuery 側で事前集計しておくと DAX が軽くなる
* DAX設計：イベントテーブルを中心に、セッションやユーザー属性をフィルターとして活用
* BigQueryでイベントデータ → セッション集計 → ユーザー集計の順に構築してからPower BIに取り込む

## 参考
* [[GA4] BigQuery Export schema](https://support.google.com/analytics/answer/7029846?hl=en#zippy=%2Cevent)
* [GA4データの分析用SQLまとめ](https://note.com/dd_techblog/n/n3e7f8c1212ef)

