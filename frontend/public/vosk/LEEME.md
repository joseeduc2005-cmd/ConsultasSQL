# Modelo de voz para navegadores sin reconocimiento propio

Chrome y Edge dictan por su cuenta y no necesitan nada de esta carpeta. Firefox y Brave
no traen reconocimiento de voz, así que se les sirve un modelo que corre dentro del
navegador — y que, a diferencia del de Chrome, no manda el audio a ningún servidor.

El archivo `modelo-es.tar.gz` (unos 40 MB) **no está en el repositorio**: no tiene por
qué viajar en cada clon. Hay que prepararlo una vez en cada entorno donde se despliegue:

    cd frontend
    node scripts/preparar-modelo-voz.mjs

Si no se hace, nada se rompe: el botón de dictar avisa de que el modelo no está
instalado en el servidor, y Chrome y Edge siguen dictando igual.
