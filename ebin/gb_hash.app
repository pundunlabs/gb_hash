{application,gb_hash,
             [{description,"Growbeard Hash Function Utility"},
              {vsn,"1.0.1"},
              {modules,[gb_hash,gb_hash_app,gb_hash_db,gb_hash_register,
                        gb_hash_sup]},
              {registered,[]},
              {applications,[kernel,stdlib,gb_log,gb_conf]},
              {mod,{gb_hash_app,[]}}]}.
