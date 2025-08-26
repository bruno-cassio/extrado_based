"""
URL configuration for extrato_app project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from . import views
from .views import index
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', index, name='index'),
    path('iniciar-extracao/', views.iniciar_extracao, name='iniciar_extracao'),
    path('baixar_resumo', views.baixar_resumo, name='baixar_resumo'),
    path('limpar_arquivos', views.limpar_arquivos, name='limpar_arquivos'),
    path('atualizar-relatorios/', views.atualizar_relatorios, name='atualizar_relatorios'),
    path('atualizar-caixa', views.atualizar_caixa, name='atualizar_caixa'),
    path('executar-atualizar-caixa', views.executar_atualizar_caixa, name='executar_atualizar_caixa'),
    path("api/buscar-cias/", views.buscar_cias_api, name="buscar_cias_api"),
    path("api/verificar-relatorios", views.verificar_relatorios_view, name="verificar_relatorios"),
    path("consultar-caixa", views.consultar_caixa_api, name="consultar_caixa_api"),
    path('api/atualizar-relatorios', views.api_atualizar_relatorios, name='api_atualizar_relatorios')
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)