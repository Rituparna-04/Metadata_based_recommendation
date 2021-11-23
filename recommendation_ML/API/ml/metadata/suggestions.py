import pandas as pd
import pickle
import psycopg2
from datetime import datetime
from psycopg2 import OperationalError, errorcodes, errors

class Recommendations:
    def __init__(self):
        path_to_artifacts = "../metadata_model/"

        filename1 = path_to_artifacts + "metadata_based_dump.sav"
        with open(filename1 , 'rb') as f1:
            self.model = pickle.load(f1)
        
        filename2 = path_to_artifacts + "metadata_mapping_dump.sav"
        with open(filename2 , 'rb') as f2:
            self.mapping = pickle.load(f2)

        
    def show_recommendations(self, course_id, nrec_items = 6):
        
        course_index = self.mapping[self.mapping['CourseId']==course_id]['CourseIndex'].values[0]
            
        #get similarity values with other movies
        similarity_score = list(enumerate(self.model[course_index]))
        similarity_score = sorted(similarity_score, key=lambda x: x[1], reverse=True)
    
        # Get the scores of the 15 most similar movies. Ignore the first movie.
        similarity_score = similarity_score[1:nrec_items]
    
        course_indices = [i[0] for i in similarity_score]
        course_list = [[self.mapping[self.mapping['CourseIndex'] == x]['CourseId'].values[0],self.mapping[self.mapping['CourseIndex'] == x]['CourseName'].values[0]]  for x in course_indices]
        return course_list
    
    def postprocessing(self, scores):
        try:
            con1 = psycopg2.connect(database="defaultdb", user='doadmin', password='g0fhkkpgixeb3xgj', host='db-postgresql-eustard-orders-do-user-9526420-0.b.db.ondigitalocean.com', port= '25060')
            con2 = psycopg2.connect(database="dbm5v75iul80su", user='imqubnmroacnpd', password='7f53c7756b302555b3ecf75f65e65faa5ef9e5e1b0c6b350e4353faa820faea4', host='ec2-34-230-115-172.compute-1.amazonaws.com', port= '5432')
        except OperationalError as err:
            con1 = None
            con2 = None
            error = str(err)
            status = "Connection failed"
            results = {}

        if con1 != None and con2 != None:    
            cursor1 = con1.cursor()
            cursor2 = con2.cursor()

            query1 = 'SELECT "SubjectId", "SubjectName", "SkillId", "SkillName" FROM public."tblCourseInfo" WHERE "CourseId" = %s'
   
            query2 = '''SELECT "tblProducts"."ProductId", "IsFree", "StandardCost" from public."tblProducts" 
                    INNER JOIN public."tblProductCourses" 
                    ON "tblProducts"."ProductId" = "tblProductCourses"."ProductId"
                    WHERE "tblProductCourses"."CourseId" = %s'''
                
            query3 = '''SELECT "tblCourseRatings"."Rating", "tblCourseReviews"."Review" FROM public."tblCourseRatings"
                    INNER JOIN public."tblCourseReviews" 
                    ON "tblCourseRatings"."CourseId" = "tblCourseReviews"."CourseId"
                    WHERE "tblCourseRatings"."CourseId" = %s'''
    
            query4 = '''SELECT "OfferName" FROM public."tblOffers"
                    WHERE "tblOffers"."ProductId" = %s
                    AND "tblOffers"."ValidFrom" <= %s
                    AND "tblOffers"."ValidTo" >= %s '''
                
            query5 = '''SELECT "PromotionName" FROM public."tblPromotions" 
                    WHERE "ProductId" = %s
                    AND "tblPromotions"."ValidFrom" <= %s
                    AND "tblPromotions"."ValidTo" >= %s '''    
                
            query6 = '''SELECT "fileId", "aboutCourse", "courseDisplayName" FROM public."course"
                    WHERE "course"."id" = %s'''     

            results = {}
            j = 1
            now = datetime.now()          
            try:
                for l in scores:
                    cursor1.execute(query1, (int(l[0]),))
                    row1 = cursor1.fetchone()
                
                    cursor1.execute(query2, (int(l[0]),))
                    row2 = cursor1.fetchone()
                
                    cursor1.execute(query3, (int(l[0]),))
                    row3 = cursor1.fetchall()
                
                    if len(row3) == 0:
                        rating = "null"
                        review = 0
                    else:
                        rating = {}
                        review = {}
                        i = 1
                        for x in row3:
                            rating[i] = float(x[0]) if x[0] is not None else "null"
                            review[i] = x[1] if x[1] is not None else 0
                            i = i + 1

                    cursor1.execute(query4, (row2[0], now, now))
                    row4 = cursor1.fetchall()
                
                    if len(row4) == 0:
                        offer = []
                    else:
                        offer = {}
                        i = 1
                        for x in row4:
                            offer[i] = x[0]
                            i = i + 1
                
                    cursor1.execute(query5, (row2[0], now, now))
                    row5 = cursor1.fetchall()
                
                    if len(row5) == 0:
                        promotion = []
                    else:
                        promotion = {}
                        i = 1
                        for x in row5:
                            promotion[i] = x[0]
                            i = i + 1

                    cursor2.execute(query6, (int(l[0]),))
                    row6 = cursor2.fetchone()
                
                    if row6 is None:
                        filepath = "null"
                        aboutcourse = "null"
                        displayname = "null"
                    else:
                        if row6[0] is not None:
                            query7 = '''SELECT "FilePath" From public."files" WHERE id = %s'''
                            cursor2.execute(query7, (int(row6[0]),))
                            row7 = cursor2.fetchone()
                            filepath = row7[0]
                        else:
                            filepath = "null"
                        
                        aboutcourse = row6[1] if row6[1] is not None else "null"
                        displayname = row6[2] if row6[2] is not None else "null"
                
                
                    results[j] = {"productId":row2[0], "courseId" : l[0], "courseName":l[1], "subjectId":row1[0], "subjectName": row1[1], 
                            "skillId":row1[2],  "skillName": row1[3], "isFree":row2[1], "standardCost":float(row2[2]), "averageRating":rating, 
                            "reviews":review, "offers":offer, "promotions":promotion, "filePath":filepath, "aboutCourse":aboutcourse, "courseDisplayName":displayname}
                    j = j+1 
            except Exception as err:
                error = str(err)
                status = "Failed Query"
                results = {}

            status = "Success"
            error = 0
            cursor1.close()
            cursor2.close()
            con1.close()
            con2.close()
        return {"results": results, "status": status, "error":error}


    def predict_recommendations(self, course):
        
        course_id = course["course_id"]
        try:
            #courses, interactions = self.prep_data()
            scores = self.show_recommendations(course_id)
            data = self.postprocessing(scores)
        except Exception as e:
            return {"results":{}, "status": "Failed", "error": str(e)}

        return data

    

   