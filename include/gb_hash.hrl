-define(LocMod, gb_hash_gen_loc_reg).
-define(DistMod, gb_hash_gen_dist_reg).

-define(LocReg, gb_hash_loc_register).
-define(DistReg, gb_hash_dist_register).

-record(gb_hash_func,{type,
                      ring}).

-record(gb_hash_register, {name,
			   func}).
