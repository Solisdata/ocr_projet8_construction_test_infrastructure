"""Script d'orchestration ETL 

Exécute le pipeline complet dans l'ordre :
    1. Tests unitaires (extract_transform/)
    2. Extract + Transform
    3. Tests d'intégration (load/)
    4. Load MongoDB
"""

import subprocess
import sys


def run_tests():
    """Lance les tests pytest"""
    print("Lancement des tests unitaires...")
    result = subprocess.run(
        ["pytest", "-v", "extract_transform/test_extract_transform.py"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    
    if result.returncode != 0:
        print("\n ÉCHEC : Les tests ont échoué !")
        print("Le pipeline ETL ne sera PAS exécuté.")
        sys.exit(1)
    
    print("\n Tous les tests sont passés !")
    return True

def run_et():
    """Lance le pipeline ETL"""
    print("\nLancement du pipeline ET...")
    
    # Importer et exécuter le main du ET
    from extract_transform.main_script_extract_transform import main
    main()
    
    print("\n Pipeline extract_transform terminé !")

def run_integration_tests():
    print("\n Lancement des tests d'intégration...")
    result = subprocess.run(
        ["python", "load/tests_integration.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print("\n --- ERREUR DÉTECTÉE DANS LE SCRIPT ---")
        print(result.stderr)  # <--- AJOUTE ÇA pour voir le Traceback Python
        print("\n ÉCHEC : Le script de mesure a planté")
        sys.exit(1)

def run_load():
    print("\n Lancement de la pipeline Load...")
    result = subprocess.run(
        ["python", "load/load_mongo.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print("\n--- DÉTAILS DE L'ERREUR DANS LOAD ---")
        print(result.stderr)
        print("\n ÉCHEC du load")
        sys.exit(1)

    print("\n Réussite du Load !")

if __name__ == "__main__":
    run_tests()    
    run_et()      # ←  ne s'exécute QUE si tests OK
    run_load()
    run_integration_tests()

