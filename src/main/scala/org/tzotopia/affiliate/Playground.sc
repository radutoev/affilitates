val row = """https://www.awin1.com/pclick.php?p=21751094339&a=598463&m=11873,"Animal Ocean Shirt LS forest green",21751094339,DE302648046,https://images.blue-tomato.com/is/image/bluetomato/302648044_front.jpg-ieqhma962p1jB4WtD4LNUcGTY34/302648044+front+jpg.jpg?$b9$,"Das wohl vielseitigstes Kleidungsstück - Ocean Hemd von AnimalHemden sind seit dem angesagten Holzfällerlook wieder voll im Trend. Kein Wunder, denn kaum ein anderes Kleidungsstück ist so universell einsetzbar wie das Hemd. Das Ocean Langarm Hemd von Animal ist der Beweis. Das Herrenhemd überzeugt durch seinen tollen Schnitt und das klassische, aber dennoch besondere Design. Eine praktische, lässige Brusttasche sorgt für den zusätzlichen Eyecatcher. Die 100% Baumwolle bieten dir höchsten Tragekomfort. Ob Hochgekrempelt, Zugeknöpft oder nur lässig über die Schulter gebunden. Deinem Style sind keine Grenzen gesetzt! FeaturesBrusttascheChambray DetailsEnzyme washedLogo Label","Streetwear > Hemden",44.95,"Blue Tomato DE",11873,,0,https://images2.productserve.com/?w=200&h=200&bg=white&trim=5&t=letterbox&url=ssl%3Aimages.blue-tomato.com%2Fis%2Fimage%2Fbluetomato%2F302648044_front.jpg-ieqhma962p1jB4WtD4LNUcGTY34%2F302648044%2Bfront%2Bjpg.jpg%3F%24b9%24&feedId=24501&k=fda1b512abf0c81f31cacb224f4bd3a1e87b4f62,EUR,,0.00,https://click.cptrack.de/?rd=true&k=lLWeGXuRXtziW26y4SJ183Icb81ftNmaI9dTR80Uhn8l_kgOTKOkgAgXjn7HDK75&rdlink=https%3A%2F%2Fwww.blue-tomato.com%2Fde-DE%2Fproduct%2F--302648046-%2F%3Fzanox%3D1%26campaign%3Dzanox%252Fde%252Ffeed%26utm_source%3Daffiliate%26utm_medium%3Dcpo%26utm_campaign%3DDE%2FZanox%26ia-pkpmtrack%3D100-6373735313236323131303-302-219-101%26_%24ja%3Dtsid%3A42904,,,EUR44.95,24501,Animal,742,"forest green",,,,,408531,,,"Versandkostenfrei ab 40 € Bestellwert",Herren,,,,,,,,,,,58.95,,,,,"Lieferzeit 1 - 3 Tage",1,,,,1,0,0,"Auf Lager",,,,,,https://images2.productserve.com/?w=70&h=70&bg=white&trim=5&t=letterbox&url=ssl%3Aimages.blue-tomato.com%2Fis%2Fimage%2Fbluetomato%2F302648044_front.jpg-ieqhma962p1jB4WtD4LNUcGTY34%2F302648044%2Bfront%2Bjpg.jpg%3F%24b9%24&feedId=24501&k=fda1b512abf0c81f31cacb224f4bd3a1e87b4f62,,,,,,,,S,,,,,,,,,5054569426875,,,,,5054569426875,"""

println(row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").size)