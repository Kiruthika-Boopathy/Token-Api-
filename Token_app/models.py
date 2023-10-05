from django.db import models


class Pagination(models.Model):
    api_id = models.CharField(primary_key=True, max_length=100)
    id = models.IntegerField()
    name = models.CharField(max_length=1000, blank=True, null=True)
    tagline = models.CharField(max_length=100, blank=True, null=True)
    first_brewed = models.CharField(max_length=100, blank=True, null=True)
    description = models.CharField(max_length=1000, blank=True, null=True)
    image_url = models.CharField(max_length=100, blank=True, null=True)
    abv = models.IntegerField(blank=True, null=True)
    ibu = models.IntegerField(blank=True, null=True)
    target_fg = models.IntegerField(blank=True, null=True)
    target_og = models.CharField(max_length=100, blank=True, null=True)
    ebc = models.IntegerField(blank=True, null=True)
    srm = models.IntegerField(blank=True, null=True)
    ph = models.CharField(max_length=100, blank=True, null=True)
    attenuation_level = models.CharField(max_length=100, blank=True, null=True)
    volume = models.CharField(max_length=100, blank=True, null=True)
    boil_volume = models.CharField(max_length=100, blank=True, null=True)
    method = models.CharField(max_length=5000, blank=True, null=True)
    ingredients = models.CharField(max_length=5000, blank=True, null=True)
    food_pairing = models.CharField(max_length=1000, blank=True, null=True)
    brewers_tips = models.CharField(max_length=555, blank=True, null=True)
    contributed_by = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'pagination'


class Insertion_data(models.Model):
    id = models.IntegerField(primary_key=True,max_length=100)
    name = models.CharField(max_length=1000, blank=True, null=True)
    tagline = models.CharField(max_length=100, blank=True, null=True)
    first_brewed = models.CharField(max_length=100, blank=True, null=True)
    description = models.CharField(max_length=1000, blank=True, null=True)
    image_url = models.CharField(max_length=100, blank=True, null=True)
    abv = models.IntegerField(blank=True, null=True)
    ibu = models.IntegerField(blank=True, null=True)
    target_fg = models.IntegerField(blank=True, null=True)
    target_og = models.CharField(max_length=100, blank=True, null=True)
    ebc = models.IntegerField(blank=True, null=True)
    srm = models.IntegerField(blank=True, null=True)
    ph = models.CharField(max_length=100, blank=True, null=True)
    attenuation_level = models.CharField(max_length=100, blank=True, null=True)
    volume = models.CharField(max_length=100, blank=True, null=True)
    boil_volume = models.CharField(max_length=100, blank=True, null=True)
    method = models.CharField(max_length=5000, blank=True, null=True)
    ingredients = models.CharField(max_length=5000, blank=True, null=True)
    food_pairing = models.CharField(max_length=1000, blank=True, null=True)
    brewers_tips = models.CharField(max_length=555, blank=True, null=True)
    contributed_by = models.CharField(max_length=100, blank=True, null=True)

