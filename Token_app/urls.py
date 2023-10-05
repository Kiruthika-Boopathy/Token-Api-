from django.urls import path
from . import views

urlpatterns=[
    path('fetch/',views.Fetch_data.as_view()),
    path('insert/',views.InsertData.as_view()),
    path('pagination/',views.InsertPageApiData.as_view())
]