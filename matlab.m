x = a(:, 1);
y = a(:, 2);
n = 1; % order of the fitted polynomial trendline
p = polyfit(x, y, n);


m = 1000; % number of trendline points (the larger the smoother)
xx = linspace(min(x), max(x), m);
yy = polyval(p, xx);

figure;
hold on;
%scatter(a(:,1), a(:,2));
plot(xx, yy, 'r-');

x = a(:, 3);
y = a(:, 4);
p = polyfit(x, y, n);
m = 1000; % number of trendline points (the larger the smoother)
yy = polyval(p, xx);
plot(xx, yy, 'b-');

title('50 users - 20 transactions')
xlabel('Conflicts (%)') % x-axis label
ylabel('Time (ms)') % y-axis label
legend('Pessimistic','Optimistic','Location','southeast')