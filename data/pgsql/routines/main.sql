create or replace function pbar ( a int )
returns int
language sql
as $$

insert into foo (val, x) values ('112', 12.2);

select a;

$$;
