import subprocess
import sys
import os

def run_tests():
    """Lance les tests pytest"""
    print("Lancement des tests unitaires...")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-v", "extract_transform/test_extract_transform.py"],
        capture_output=True,
        text=True
    )
    
    print(result.stdout)
    
    if result.returncode != 0:
        print("\n ÉCHEC : Les tests ont échoué !")
        print(result.stderr)
        sys.exit(1)
    
    print("\n Tous les tests sont passés !")
    return True

def run_et():
    """Lance le pipeline ETL"""
    print("\nLancement du pipeline ET...")
    from extract_transform.main_script_extract_transform import main
    main()
    print("\n Pipeline extract_transform terminé !")

def run_integration_tests():
    print("\n Lancement des tests d'intégration...")
    result = subprocess.run(
        [sys.executable, "load/tests_integration.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print("\n --- ERREUR DÉTECTÉE DANS LES TESTS D'INTÉGRATION ---")
        print(result.stderr)
        sys.exit(1)

def run_load():
    print("\n Lancement de la pipeline Load...")
    result = subprocess.run(
        [sys.executable, "load/load_mongo.py"],
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
    run_et()
    run_tests() 
    run_load()
    run_integration_tests()