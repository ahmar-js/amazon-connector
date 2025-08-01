from rest_framework import serializers
from django.utils import timezone
from .models import (
    Activities, 
    MarketplaceLastRun
)
import json
from datetime import datetime


class ActivitiesSerializer(serializers.ModelSerializer):
    marketplace_name = serializers.ReadOnlyField()
    duration_formatted = serializers.ReadOnlyField()
    total_records = serializers.ReadOnlyField()
    
    class Meta:
        model = Activities
        fields = '__all__'
        read_only_fields = ['activity_id', 'created_at', 'updated_at']


class MarketplaceLastRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = MarketplaceLastRun
        fields = '__all__' 