import pandas as pd



# 데이터 불러오기

rating_df = pd.read_csv('C:/Users/User/Desktop/dataset/movie_lense_100k/u.data', sep = '\t', header = None)
rating_df.columns = ['user_id', 'item_id', 'rating', 'timestamp'] # 리뷰자 id, 영화 id, 별점, 리뷰기록시간

user_df = pd.read_csv('C:/Users/User/Desktop/dataset/movie_lense_100k/u.user', sep = '|', header = None)
user_df.columns = ['user_id', 'age', 'gender', 'occupation', 'zip_code'] # 나이, 성별, 직업, 어디사는지

movie_df = pd.read_csv('C:/Users/User/Desktop/dataset/movie_lense_100k/u.item', sep = '|', header = None, encoding = 'latin_1')
movie_df.columns = ['movie_id' , 'movie_title' , 'release_date' , 'video_release_date' ,
              'IMDb_URL' , 'unknown' , 'Action' , 'Adventure' , 'Animation' ,
              'Children' , 'Comedy' , 'Crime' , 'Documentary' , 'Drama' , 'Fantasy' ,
              'Film-Noir' , 'Horror' , 'Musical' , 'Mystery' , 'Romance' , 'Sci-Fi' ,
              'Thriller' , 'War' , 'Western']



# movie_df에 각 영화의 평균 평점 데이터 컬럼 추가

    # 영화별 평점평균
avg_rating = rating_df.groupby('item_id', as_index = False).mean()[['item_id', 'rating']]
avg_rating.rename({'item_id':'movie_id'}, axis = 1, inplace = True) # merge하기위한 컬럼이름 통일

    # movie_df에 영화별 평점평균 merge
movie_df = movie_df.merge(avg_rating, on = 'movie_id')



# 당첨자 3명
winner = [640, 269, 155]

# 당첨자를 위한 추천영화
recom_movies = {winner[0] : {},
                winner[1] : {},
                winner[2] : {}}
# 추천 영화 개수
recom_num_total = 4

for i in winner:

    # ---당첨자 정보---

    # 1) 당첨자 나이
    winner_age = user_df[user_df['user_id'] == i]['age'].values[0]

    # 2) 당첨자 연령대
    winner_agegroup = (winner_age//10)*10

    # 3) 당첨자 성별
    winner_gender = user_df[user_df['user_id'] == i]['gender'].values[0]

    print(f'\n======== <당첨자 {i}> ========\n나이: {winner_age} / 성별: {winner_gender}')



    # ---당첨자 선호 영화---

    # 1) 당첨자가 평가한 영화 중, 4점 이상 평가한 영화 (4점 이상 평가했을 때 선호하는 영화라고 판단)
    winner_rating = rating_df[(rating_df['user_id'] == i)\
                              &(rating_df['rating'] >= 4)]

    # 단, 1)에서 당첨자가 평가한 영화가 3개 이하면, 3점 이상 평가한 영화
    if len(winner_rating) <= 3:
        winner_rating = rating_df[(rating_df['user_id'] == i)\
                                  &(rating_df['rating'] >= 3)]


    # 2) 해당 영화의 제목 및 정보 : isin 이용
    winner_movie = movie_df[movie_df['movie_id'].isin(winner_rating['item_id'])]

    # 혹은 2) 해당 영화의 제목 및 정보 : merge 이용
    movie_df_copy = movie_df.copy()
    movie_df_copy.rename(columns = {'movie_id':'item_id'}, inplace = True) # merge 전에 컬럼 이름 통일
    winner_movie = movie_df_copy.merge(winner_rating, on = 'item_id')


    # 3) 당첨자의 장르 선호도
    winner_genre = winner_movie.sum()[6:24].sort_values(ascending = False)

    # 단, 3)에서 같은 순위가 있을 경우, 전체 평가자가 선호하는(4~5점대) 장르 순으로 정렬

        # 3-a) 전체 평가자가 4점 이상 평가한 영화의 id
    all_rating = rating_df[(rating_df['rating'] >= 4)]

        # 3-b) 해당 영화의 제목 정보 : isin 이용
    all_movie =  movie_df[movie_df['movie_id'].isin(all_rating['item_id'])]

        # 3-c) 전체 평가자의 장르 선호도
    all_genre = all_movie.sum()[6:24].sort_values(ascending = False)

        # 3-d) 정렬 1순위: 당첨자의 장르 선호도 / 2순위: 전체 평가자의 장르 선호도
    winner_genre_df = winner_genre.reset_index()
    winner_genre_df.rename({'index':'genre', 0:'winner'}, axis = 1, inplace = True)
    all_genre_df = all_genre.reset_index()
    all_genre_df.rename({'index':'genre', 0:'all'}, axis = 1, inplace = True)

    genre_df = winner_genre_df.merge(all_genre_df, on = 'genre') # 병합
    genre_df.sort_values(by = ['winner', 'all'], ascending = False, inplace = True) # 정렬

        # 3-e) 만약 당첨자가 한번도 안 본 장르면 제거
    genre_df = genre_df[genre_df['winner'] != 0]

    print('선호 장르 순위')
    print(genre_df)



    # --- 당첨자가 선호하는 장르를 가장 많이 포함한 영화 --> 추천영화 ---
    # 단, 평균평점이 3점 이상이어야 추천영화에 포함될 수 있음
    # 해당장르 포함한 영화가 4개 초과일 경우, 평점 순으로 상위 4개 포함

    # 현재 영화추천개수
    recom_num_current = 0

    # 선호하는 장르를 전부 포함한 영화 (없으면 반복)
    for j in range(len(genre_df.genre), 0, -1):
        recom_movie = movie_df[(movie_df[genre_df.genre[:j]] == 1).all(axis = 1)] # 해당 장르 == 1을 만족하는 모든 영화
        recom_movie.sort_values('rating', ascending = False, inplace = True) # 평점순 정렬
        recom_movie = recom_movie[recom_movie['rating'] >= 4.0] # 평점 4점 이상 영화만 해당

        if recom_movie.empty == True: # 만족하는 영화가 없으면
            pass

        else: # 만족하는 영화가 있으면
            genre_str = ' '.join(list(genre_df.genre[:j])) # 해당 장르
            recom_movies[i].update({genre_str : list(recom_movie['movie_title'])[:(recom_num_total - recom_num_current)]}) # 해당 장르 : 영화제목[영화개수]
            recom_num_current += len(recom_movies[i][genre_str]) # 현재 영화추천개수 갱신
        if recom_num_current >= recom_num_total: # 추천개수만큼 모았으면 break
            break

print(recom_movies)