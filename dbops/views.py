from django.http import HttpResponse, JsonResponse


def health_check(request):
    return JsonResponse("DBOPS Project: I am up and doing fine", safe=False)