from rest_framework import views
from rest_framework.generics import CreateAPIView,ListCreateAPIView,ListAPIView
from rest_framework.response import Response
from . import serializers
from .models import Pagination
from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User
import time
import requests
from concurrent.futures import ThreadPoolExecutor
import json
from rest_framework import status
import concurrent
from django.db import  connection
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated



class Fetch_data(ListAPIView):
    # permission_classes = [IsAuthenticated]
    # authentication_classes = [TokenAuthentication]
    serializer_class = serializers.Serial
    queryset = Pagination.objects.all()
    def get(self, request,*args,**kwargs):
        query_set = Pagination.objects.all()
        serializer = serializers.Serial(query_set, many=True)
        user=User.objects.get(username="kiruthika")
       # token = Token.objects.create(user=user)
        return Response(serializer.data)


class InsertData(views.APIView):
    def get(self, request):
        start = time.time()

        def insert_data(data):
            with connection.cursor() as cursor:
                api_data = data
                for item in api_data:
                    print(item)
                    raw_query = """
                                    INSERT INTO token_app_insertion_data (
                                         id, name, tagline, first_brewed, description, image_url,
                                        abv, ibu, target_fg, target_og, ebc, srm, ph, attenuation_level,
                                        volume, boil_volume, method, ingredients, food_pairing,
                                        brewers_tips, contributed_by
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                         %s, %s, %s, %s, %s
                                    )
                                """
                    params = (

                        item["id"],
                        item["name"],
                        item["tagline"],
                        item["first_brewed"],
                        item["description"],
                        item["image_url"],
                        item["abv"],
                        item["ibu"],
                        item["target_fg"],
                        item["target_og"],
                        item["ebc"],
                        item["srm"],
                        item["ph"],
                        item["attenuation_level"],
                        str(item["volume"]),
                        str(item["boil_volume"]),
                        str(item["method"]),
                        str(item["ingredients"]),
                        str(item["food_pairing"]),
                        item["brewers_tips"],
                        item["contributed_by"]
                    )

                    cursor.execute(raw_query, params)

        def fetch_data(url):
            try:
                headers = {
                    "Authorization": "Token 8296064da8fa7b67dff49d2b177d7570ed7e1bc6"
                }
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an exception for non-200 status codes
                data = response.json()
                insert_data(data)
                return {"message": "success"}
            except requests.exceptions.RequestException as e:
                return {'error': f'Failed to retrieve data from {url}: {str(e)}'}
            except json.JSONDecodeError as e:
                return {'error': f'Failed to parse JSON response from {url}: {str(e)}'}

        api_urls = ['http://127.0.0.1:8000/fetch/',
                    'http://127.0.0.1:8000/fetch/',
                    'http://127.0.0.1:8000/fetch/',
                    'http://127.0.0.1:8000/fetch/'

                    ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(api_urls)) as executor:
            results = list(executor.map(fetch_data, api_urls))
            # concurrent.futures.wait(results)
        data = [result for result in results]
        end = time.time()
        tot = end - start
        # val = {"total": tot, "data": "Data inserted successfully"}

        return Response(data, status=status.HTTP_201_CREATED)

