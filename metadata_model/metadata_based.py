#importing necessary libraries

import pandas as pd
import pickle
import os
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

path = os.getcwd() + "//OneDrive//Desktop//Flotilla Techs//Metadata_based_recommendation//metadata_model"
course_metadata = pd.read_csv(path + '//course_info.csv')
course_metadata.columns
course_metadata['metadata'] = course_metadata.apply(lambda x : ''.join(x['CourseName']) + ' ' + ''.join(x['SubjectName']) + ' ' + ''.join(x['SkillName']) + ' ' + ''.join(x['ProviderName']), axis = 1)
course_metadata

count_vec = CountVectorizer(stop_words='english')
count_vec_matrix = count_vec.fit_transform(course_metadata['metadata'])

cosine_sim_matrix = cosine_similarity(count_vec_matrix, count_vec_matrix)

filename = path + "//metadata_based_dump.sav"
with open(filename, 'wb') as f:
    pickle.dump(cosine_sim_matrix, f)
    
#movies index mapping
mapping = pd.Series(course_metadata.index,index = course_metadata['CourseName'])

filename = path + "//metadata_mapping_dump.sav"
with open(filename, 'wb') as f:
    pickle.dump(mapping, f)

#recommender function to recommend movies based on metadata
def recommend_movies_based_on_metadata(course_input):
    movie_index = mapping["clinical chem"]
    
    #get similarity values with other movies
    similarity_score = list(enumerate(cosine_sim_matrix[movie_index]))
    similarity_score = sorted(similarity_score, key=lambda x: x[1], reverse=True)
    
    # Get the scores of the 15 most similar movies. Ignore the first movie.
    similarity_score = similarity_score[1:6]
    
    movie_indices = [i[0] for i in similarity_score]
    return (course_metadata['CourseName'].iloc[movie_indices])

