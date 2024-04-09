# -*- coding: utf-8 -*-
from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('test/', views.test, name='test'),
    path('search/', views.search, name='search'),
    # path('data/', views.data, name='data'),
    path('set_view_sp/', views.set_view_sp, name='set_view_sp'),
]