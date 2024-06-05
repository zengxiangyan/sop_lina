# -*- coding: utf-8 -*-
from django.urls import path
from django.conf import settings
from django.conf.urls.static import static
from . import views

urlpatterns = [
    path('', views.get_uuid, name='get_uuid'),
    path('save/', views.save, name='save'),
    path('search/', views.search, name='data'),
    path('media/', views.download_file, name='download_file'),
    path('add/', views.add, name='add'),
    path('lvname/', views.sop_lv_name, name='lv_name'),
    path('upload/', views.upload, name='upload'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
