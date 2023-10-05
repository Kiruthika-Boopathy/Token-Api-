from rest_framework import serializers
from .models import *

class Serial(serializers.ModelSerializer):
    class Meta:
        model = Pagination
        fields = '__all__'
