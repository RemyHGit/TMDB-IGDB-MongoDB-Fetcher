import threading

from movies_mongodb_script import compare_movies_file_db_add_db as compare_movies
from series_mongodb_script import compare_series_file_db_add_db as compare_series


try:
    # Run the program in a loop to keep inserting and updating the data
        while True:
            # Create threads for each function
            compare_movies_thread = threading.Thread(target=compare_movies)
            compare_series_thread = threading.Thread(target=compare_series)


            # Start the threads
            compare_movies_thread.start()
            compare_series_thread.start()
            # movie_update_thread.start()
            # serie_update_thread.start()

            # Wait for the update thread to finish
            compare_movies_thread.join()
            compare_series_thread.join()
            # movie_update_thread.join()
            # serie_update_thread.join()

            if not (compare_movies_thread.is_alive() or compare_series_thread.is_alive()): # or movie_update_thread.is_alive() or serie_update_thread.is_alive()):
                print("All data has been updated in the database, processing to update:")
                from movies_mongodb_script import movies_update_db as movie_update
                from series_mongodb_script import series_update_db as serie_update
                movie_update_thread = threading.Thread(target=movie_update)
                serie_update_thread = threading.Thread(target=serie_update)
                
                movie_update_thread.start()
                serie_update_thread.start()
                
                movie_update_thread.join()
                serie_update_thread.join()
                break
            else:
                print("Data update failed")
            

except Exception as e:
    print(f"Error processing conjecture: {e}")