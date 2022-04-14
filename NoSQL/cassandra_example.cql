create keyspace mykeyspace with replication = {'class':'SimpleStrategy','replication_factor' : 2};

use mykeyspace;

create table usertable (userid int primary key, usergivenname varchar, userfamilyname varchar, userprofession varchar);

insert into usertable (userid, usergivenname, userfamilyname, userprofession) values (1, 'Oliver', 'Veits', 'Freelancer');

update usertable set userprofession = 'IT Consultant' where userid = 1;

select * from usertable where userid = 1;

select * from usertable where userprofession = 'IT Consultant';

create index idx_dept on usertable(userprofession);

select * from usertable where userprofession = 'IT Consultant';
