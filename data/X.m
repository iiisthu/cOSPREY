x = [32, 40, 48, 56, 64];
m = 2442878194;
y = [m/(49*60+53), m/(48*60+25), m/(41*60+4), m/(37*60+12), m/(34*60+44)];
f = inline('x./(a(1)*x+a(2))', 'a', 'x');
a = nlinfit(x, y, f, [3.64*1e-7 5.51*1e-5]);
xx = linspace(0, 70, 1000);
yy = xx./(a(1)*xx+a(2));
subplot(1, 2, 2);
plot(xx, yy, 'r--', x, y, 'o');
xlabel ('number of machines', 'FontSize',24.0)
ylabel ('number of state space developed per second', 'FontSize',24.0)
title ('3JTZ','FontSize',24.0)
x = [8, 16, 32, 64];
m = 1320266262;
y = [m/(97*60+41), m/(56*60+1), m/(36*60+36), m/(26*60+57)];
f = inline('x./(a(1)*x+a(2))', 'a', 'x');
a = nlinfit(x, y, f, [3.64*1e-7 5.51*1e-5]);
xx = linspace(0, 70, 1000);
yy = xx./(a(1)*xx+a(2));
subplot(1, 2, 1);
plot(xx, yy, 'r--', x, y, 'o');
xlabel ('number of machines', 'FontSize',24.0)
ylabel ('number of state space developed per second', 'FontSize',24.0)
title ('1T8K','FontSize',24.0)