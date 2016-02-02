x = [8, 16, 32, 64];
m = 1320266262;
y = [m/(97*60+41), m/(56*60+1), m/(36*60+36), m/(26*60+57)];
f = inline('x./(a(1)*x+a(2))', 'a', 'x');
a = nlinfit(x, y, f, [3.64*1e-7 5.51*1e-5]);
xx = linspace(0, 64, 1000);
yy = xx./(a(1)*xx+a(2));
plot(xx, yy, 'r-', x, y, 'k.');
xlabel ('number of machines')
ylabel ('number of state space developed per second')