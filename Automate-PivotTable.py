import vaex as vx
import pandas as pd
from datemerge import datemerge
from hyperlink import hyperlink
from to_excel_file import to_excel_file

    # Counter Set-up:
slide_n_df_dict = {}
slide_counter = 4

topic_list = ['ShopeeFood', 'GrabFood', 'GoFood', 'Baemin', 'BeFood']
channel_list = ['Facebook', 'Owned channel', 'Tiktok', 'News', 'Forum', 'Instagram', 'Youtube', 'E-commerce']
sent_list = ['Positive', 'Neutral', 'Negative']
voice_list = ['Brand', 'Merchant', 'Shipper', 'User']
target_mother_label_list = ['Service', 'Fanpage Activity', 'Campaign', 'Rider', 'Merchant', 'App Experience']
label_classify_df = pd.read_excel(r"C:\Users\Admin\Desktop\SPFlabels.xlsx", sheet_name='Label classify')


file_name1 = r'C:\Users\Admin\Desktop\SPF1824.xlsx'
file_name2 = r'C:\Users\Admin\Desktop\SPFCom1824.xlsx'

# =============================:

df1 = vx.from_pandas(pd.read_excel(file_name1, sheet_name='Data'))
df2 = vx.from_pandas(pd.read_excel(file_name1, sheet_name='Minigame1'))
df3 = vx.from_pandas(pd.read_excel(file_name1, sheet_name='Minigame2'))
df4 = vx.from_pandas(pd.read_excel(file_name2, sheet_name='Data'))

df = vx.concat([df1, df2, df3, df4])
# ============================= SLIDE 4:
buzz_df = df.groupby(by='Topic').agg({'Id': 'count'}).to_pandas_df().set_index('Topic').reindex(topic_list)
slide_n_df_dict['Slide 4'] = buzz_df

# ============================= SLIDE 5:
df_exclude_minigame = df[(df.Minigame != 'Minigame') & (df.Minigame != 'minigame')]
buzz_df_exclude_minigame = df_exclude_minigame.groupby(by='Topic').agg({'Id': 'count'}).to_pandas_df().set_index('Topic').reindex(topic_list)
slide_n_df_dict['Slide 5'] = buzz_df_exclude_minigame

# ==============================

channel_df = df_exclude_minigame.groupby(by='Channel').agg({'Id': 'count'})
channel_df = channel_df.to_pandas_df().set_index('Channel').reindex(channel_list).fillna(0)
channel_pct_df = channel_df.apply(lambda x: round(x/channel_df.sum().sum(), 3))
slide_n_df_dict['Slide 6'] = channel_pct_df

# ==============================
Slide_7_list = []
for each_channel in ['News', 'Owned channel', 'Facebook']:
    # Absolute:
    brand_by_channel_df = df_exclude_minigame[df_exclude_minigame.Channel == each_channel].groupby(by='Topic').agg({'Id': 'count'}).to_pandas_df().set_index('Topic').reindex(topic_list).fillna(0)
    brand_by_channel_df.index.name = each_channel
    brand_by_channel_df.loc['Total', :] = brand_by_channel_df['Id'].sum().sum()

    Slide_7_list.append(brand_by_channel_df)

    # Voice break-down:
    if each_channel in ['Facebook', 'Owned channel']:
        voice_df = df_exclude_minigame[df_exclude_minigame.Channel == each_channel].groupby(by=['Channel', 'Voice', 'Topic']).agg({'Id': 'count'}).to_pandas_df()
        voice_df = voice_df.pivot_table(index='Voice', values='Id', columns='Topic', aggfunc='sum').reindex(['Shipper', 'User', 'Merchant', 'Brand']).reindex(topic_list, axis=1).fillna(0)
        voice_df.index.name = each_channel
        Slide_7_list.append(voice_df)
slide_n_df_dict['Slide 7'] = Slide_7_list

# ============================== SENTIMENT (Slide7):

overall_sent_df = df.groupby(by='Sentiment').agg({'Id': 'count'}).to_pandas_df().set_index('Sentiment').reindex(sent_list).fillna(0)
overall_sent_df = overall_sent_df.apply(lambda x: round(x/overall_sent_df.sum().sum(), 5))

# ============================== TREND-LINE (Slide9):
slide_n_df_dict['Slide 9'] = datemerge(df_exclude_minigame.groupby(['PublishedDate', 'Topic']).agg({'Id': 'count'}).to_pandas_df().pivot_table(
    index='PublishedDate', values='Id', aggfunc='sum', columns='Topic').reindex(topic_list, axis=1), '2023-09-18', '2023-09-24')

# ============================== CHANNEL BY BRAND (Slide10):
Slide_10_list = []
total_buzz_exclude_minigame_by_brand_df = df_exclude_minigame.groupby('Topic').agg({'Id': 'count'}).to_pandas_df().pivot_table(columns='Topic', values='Id', aggfunc='sum').reindex(topic_list, axis=1)
Slide_10_list.append(total_buzz_exclude_minigame_by_brand_df)

brand_buzz_by_channel_df = df_exclude_minigame.groupby(['Topic', 'Channel']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Topic', columns='Channel', values='Id', aggfunc='sum')
brand_buzz_by_channel_df = brand_buzz_by_channel_df.apply(lambda x: round(x/brand_buzz_by_channel_df.sum(axis=1), 5)).reindex(topic_list).reindex(channel_list, axis=1).fillna(0)
brand_buzz_by_channel_df['Total'] = brand_buzz_by_channel_df.sum(axis=1)
Slide_10_list.append(brand_buzz_by_channel_df)

brand_buzz_by_voice_df = df_exclude_minigame.groupby(['Topic', 'Voice']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Topic', columns='Voice', values='Id', aggfunc='sum')
brand_buzz_by_voice_df = brand_buzz_by_voice_df.apply(lambda x: round(x/brand_buzz_by_voice_df.sum(axis=1), 5)).reindex(topic_list).reindex(voice_list, axis=1).fillna(0)
brand_buzz_by_voice_df['Total'] = brand_buzz_by_voice_df.sum(axis=1)
Slide_10_list.append(brand_buzz_by_voice_df)
slide_n_df_dict['Slide 10'] = Slide_10_list

        # SLIDE 11, 12:

Slide_11_12_dict = {'Slide 11': df, 'Slide 12': df_exclude_minigame}
for slide, working_df in Slide_11_12_dict.items():
    df_list = []

    # SENTIMENT BY BRAND:
    brand_buzz_by_sent_df = working_df.groupby(['Topic', 'Sentiment']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Topic', columns='Sentiment', values='Id', aggfunc='sum')
    brand_buzz_by_sent_df = brand_buzz_by_sent_df.apply(lambda x: round(x / brand_buzz_by_sent_df.sum(axis=1), 5)).reindex(topic_list).reindex(sent_list, axis=1).fillna(0)
    brand_buzz_by_sent_df['Total'] = brand_buzz_by_sent_df.sum(axis=1)
    df_list.append(brand_buzz_by_sent_df)

    # BUZZ BY BRAND:
    total_buzz_by_brand_df = working_df.groupby('Topic').agg({'Id': 'count'}).to_pandas_df().pivot_table(columns='Topic', values='Id', aggfunc='sum').reindex(topic_list, axis=1)
    df_list.append(total_buzz_by_brand_df)

    # LABELS BY SENTIMENT BY BRAND:
    for brand in topic_list:
        brand_working_df = working_df[working_df['Topic'] == brand]
        constant = int(brand_working_df['Id'].count())/int(brand_working_df['Sentiment'].count())
        all_labels_df = pd.DataFrame(columns=sent_list)

        for col_name in ['Service', 'Campaign', 'Labels2', 'Labels4', 'Labels6', 'Labels8', 'Labels10']:
            all_labels_df = pd.concat([all_labels_df, brand_working_df.groupby([col_name, 'Sentiment']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index=col_name, columns='Sentiment', values='Id', aggfunc='sum').reindex(sent_list, axis=1).fillna(0)])

        all_labels_df = pd.DataFrame(all_labels_df).apply(lambda x: x.astype(int) * constant).reset_index().rename({'index': 'SonLabels'}, axis=1).merge(label_classify_df, on='SonLabels', how='left')
        all_labels_df = pd.DataFrame(all_labels_df.groupby(['MotherLabels', 'SonLabels']).sum()).reindex(sent_list, axis=1)
        all_labels_df['Total'] = all_labels_df.sum(axis=1)
        all_labels_df = all_labels_df.reset_index().merge(pd.DataFrame(all_labels_df.groupby(['MotherLabels'])['Total'].sum()).reset_index().rename({'Total': 'TotalByMoLabels'}, axis=1), on='MotherLabels', how='left').sort_values(by=['TotalByMoLabels', 'Total'], ascending=[True, True]).drop('TotalByMoLabels', axis=1)

        mother_labels_df = all_labels_df.groupby('MotherLabels').sum().drop('SonLabels', axis=1).sort_values(by='Total', ascending=True).apply(lambda x: round(x/int(brand_working_df['Id'].count()), 5))
        mother_labels_df.index.name = brand
        df_list.append(mother_labels_df)

    # VOICE BY SENTIMENT BY BRAND:
        voice_by_sent_by_brand = brand_working_df.groupby(['Voice', 'Sentiment']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Voice', columns='Sentiment', values='Id', aggfunc='sum').reindex(voice_list).reindex(sent_list, axis=1).fillna(0)
        voice_by_sent_by_brand = voice_by_sent_by_brand.apply(lambda x: round(x/voice_by_sent_by_brand.sum(axis=1), 4)).fillna(0)
        voice_by_sent_by_brand.index.name = brand
        df_list.append(voice_by_sent_by_brand)

    # BUZZ BY VOICE BY BRAND:
        buzz_by_voice_by_brand = brand_working_df.groupby('Voice').agg({'Id': 'count'}).to_pandas_df().set_index('Voice').reindex(voice_list[::-1])
        buzz_by_voice_by_brand.index.name = brand
        df_list.append(buzz_by_voice_by_brand)

    slide_n_df_dict[slide] = df_list

# ============================== CAMPAIGN ALL (SLIDE 13):
Slide_13_list = []
for brand in ['ShopeeFood', 'GrabFood', 'GoFood', 'Baemin']:
    campaign_df = df[df['Topic'] == brand].groupby('Campaign').agg({'Id': 'count'}).to_pandas_df().set_index('Campaign')
    campaign_df = campaign_df.reindex([x for x in list(campaign_df.index) if x is not None])
    total_buzz = campaign_df['Id'].sum().sum()
    campaign_df = campaign_df.apply(lambda x: round(x/campaign_df.sum().sum(), 4))
    campaign_df.index.name = brand
    campaign_df.loc['Total', 'Id'] = total_buzz
    Slide_13_list.append(campaign_df)
slide_n_df_dict['Slide 13'] = Slide_13_list

# ============================== CAMPAIGN EXCLUDE MINIGAME (SLIDE 14):
Slide_14_list = []
for brand in ['ShopeeFood', 'GrabFood', 'GoFood', 'Baemin']:
    campaign_exclude_minigame_df = df_exclude_minigame[df_exclude_minigame['Topic'] == brand].groupby('Campaign').agg({'Id': 'count'}).to_pandas_df().set_index('Campaign')
    campaign_exclude_minigame_df = campaign_exclude_minigame_df.reindex([x for x in list(campaign_exclude_minigame_df.index) if x is not None])
    total_buzz = campaign_exclude_minigame_df['Id'].sum().sum()
    campaign_exclude_minigame_df = campaign_exclude_minigame_df.apply(lambda x: round(x/campaign_exclude_minigame_df.sum().sum(), 4))
    campaign_exclude_minigame_df.index.name = brand
    campaign_exclude_minigame_df.loc['Total', 'Id'] = total_buzz
    Slide_14_list.append(campaign_exclude_minigame_df)
slide_n_df_dict['Slide 14'] = Slide_14_list

# ============================== (SHOPEEFOOD ONLY) SENTIMENT BREAK-DOWN BY VOICE (SLIDE 16):
Slide_16_list = []
spf_df = df[df['Topic'] == 'ShopeeFood']
buzz_by_voice_df = spf_df.groupby(['Voice']).agg({'Id': 'count'}).to_pandas_df().pivot_table(columns='Voice', values='Id', aggfunc='sum').reindex(['User', 'Merchant', 'Shipper'], axis=1)
Slide_16_list.append(buzz_by_voice_df)

voice_by_sent_df = spf_df.groupby(['Voice', 'Sentiment']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Voice', columns='Sentiment', values='Id', aggfunc='sum')
voice_by_sent_df = voice_by_sent_df.apply(lambda x: round(x/voice_by_sent_df.sum(axis=1), 5)).reindex(['User', 'Merchant', 'Shipper']).reindex(sent_list, axis=1).fillna(0)
voice_by_sent_df['Total'] = voice_by_sent_df.sum(axis=1)
Slide_16_list.append(voice_by_sent_df)

buzz_by_voice_2_df = spf_df.groupby(['Voice']).agg({'Id': 'count'}).to_pandas_df().set_index('Voice').reindex(['User', 'Merchant', 'Shipper', 'Brand'])
Slide_16_list.append(buzz_by_voice_2_df)

slide_n_df_dict['Slide 16'] = Slide_16_list

# ============================= TOP TOPICS (SLIDE 18):
Slide_18_list = []
for voice in ['User', 'Merchant', 'Shipper']:
    for sent in ['Positive', 'Negative']:
        exclude_minigame_voice_sent_df = df_exclude_minigame[(df_exclude_minigame['Voice'] == voice) & (df_exclude_minigame['Sentiment'] == sent)]
        top_sources = exclude_minigame_voice_sent_df.groupby(['Title', 'Channel', 'UrlTopic']).agg({'Id': 'count'}).sort(by='Id', ascending=False).head(5).to_pandas_df().set_index(['Title', 'Channel', 'UrlTopic'])
        top_sources = top_sources.apply(lambda x: round(x/top_sources.sum().sum(), 5)).reset_index()
        top_sources['Link'] = None
        for i in range(len(top_sources.axes[0])):
            top_sources.iloc[i, -1] = ('=' + 'HYPERLINK(' + '"' + str(top_sources.iloc[i, 2]) + '",' + '"' + str(top_sources.iloc[i, 0])[:30] + ' ...' + '")')
        top_sources = top_sources.drop(['Title', 'UrlTopic'], axis=1).reindex(['Link', 'Id', 'Channel'], axis=1).set_index(['Link'])
        top_sources.index.name = str(voice) + '+' + str(sent)
        Slide_18_list.append(top_sources)

slide_n_df_dict['Slide 18'] = Slide_18_list

# ============================= SLIDE 20:
df_exclude_minigame_food_only = df_exclude_minigame[df_exclude_minigame.Service == 'Food']

buzz_df_exclude_minigame_food_only = df_exclude_minigame_food_only.groupby(by='Topic').agg({'Id': 'count'}).to_pandas_df().set_index('Topic').reindex(topic_list)
slide_n_df_dict['Slide 20'] = buzz_df_exclude_minigame_food_only

# ============================= SLIDE 21:
Slide_21_list = []
total_buzz_exclude_minigame_food_only_by_brand_df = df_exclude_minigame_food_only.groupby('Topic').agg({'Id': 'count'}).to_pandas_df().pivot_table(columns='Topic', values='Id', aggfunc='sum').reindex(topic_list, axis=1)
Slide_21_list.append(total_buzz_exclude_minigame_food_only_by_brand_df)

brand_buzz_by_channel_food_only_df = df_exclude_minigame_food_only.groupby(['Topic', 'Channel']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Topic', columns='Channel', values='Id', aggfunc='sum')
brand_buzz_by_channel_food_only_df = brand_buzz_by_channel_food_only_df.apply(lambda x: round(x/brand_buzz_by_channel_food_only_df.sum(axis=1), 5)).reindex(topic_list).reindex(channel_list, axis=1).fillna(0)
brand_buzz_by_channel_food_only_df['Total'] = brand_buzz_by_channel_food_only_df.sum(axis=1)
Slide_21_list.append(brand_buzz_by_channel_food_only_df)

brand_buzz_by_voice_food_only_df = df_exclude_minigame_food_only.groupby(['Topic', 'Voice']).agg({'Id': 'count'}).to_pandas_df().pivot_table(index='Topic', columns='Voice', values='Id', aggfunc='sum')
brand_buzz_by_voice_food_only_df = brand_buzz_by_voice_food_only_df.apply(lambda x: round(x/brand_buzz_by_voice_food_only_df.sum(axis=1), 5)).reindex(topic_list).reindex(voice_list, axis=1).fillna(0)
brand_buzz_by_voice_food_only_df['Total'] = brand_buzz_by_voice_food_only_df.sum(axis=1)
Slide_21_list.append(brand_buzz_by_voice_food_only_df)

slide_n_df_dict['Slide 21'] = Slide_21_list

to_excel_file('SPF1824OutPut', slide_n_df_dict)
