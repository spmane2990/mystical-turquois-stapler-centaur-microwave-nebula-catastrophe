Here are complete, self-explanatory study notes covering all the topics from the document, organized logically by subject area.

---

# Mathematics Study Notes

---

## 1. Numbers & Basic Algebraic Operations

### **Number System Classification**

Numbers are broadly classified into Real and Imaginary numbers:

| Number Type      | Description & Characteristics                                                       | Examples / Details |
| ---------------- | ----------------------------------------------------------------------------------- | ------------------ |
| **Real Numbers** | Numbers that exist on the real number line. Includes rational & irrational numbers. |

| Includes integers, fractions, non-terminating decimals.

|
| **Rational Numbers** | Numbers expressible as a ratio $\frac{a}{b}$ where $b \neq 0$.

<br>

<br>Decimal expansions either terminate or repeat.

| $\frac{9}{6}$; $\frac{1}{3} = 0.333... = 0.\overline{3}$;

<br>

<br>$\frac{1}{9} = 0.\overline{1}$; $\frac{1}{11} = 0.\overline{09}$

|
| **Irrational Numbers** | Numbers with non-repeating, non-terminating decimals. Cannot be expressed as exact fractions.

| $\sqrt{2}$, $\pi$ (Pi)

|
| **Imaginary Numbers** | Square roots of negative numbers. Uses $i = \sqrt{-1}$.

| $\sqrt{-4} = 2i$<br> |

### **Radical Expressions & Operations**

- **Radical Sign & Radicand:** In $\sqrt[b]{x^a}$, $\sqrt{\ }$ is the **radical sign**, $x^a$ is the **radicand**, and $b$ is the root index.

- **Fractional Exponent Rule:** $x^{\frac{a}{b}} = \sqrt[b]{x^a}$.

- **Number of Real Solutions:**
- **Odd roots** have **one real solution**.

- **Even roots** have **two real solutions** (positive and negative).

- **Conjugates:** Binomials with opposite signs in the middle, $(x-y)$ and $(x+y)$. Useful for rationalizing radical expressions and complex numbers.

### **Complex Numbers**

- **Standard Form:** $a + bi$, where $a$ is the real part and $bi$ is the imaginary part.

- If $b=0$, it is a **pure real number**.

- If $a=0$, it is a **pure imaginary number**.

- **Powers of $i$:** $i = \sqrt{-1}$, $i^2 = -1$, $i^3 = -i$, $i^4 = 1$.

- **Division / Rationalization:** Multiply the numerator and denominator by the **complex conjugate** of the denominator.

$$\frac{5+2i}{3-i} \cdot \frac{3+i}{3+i} = \frac{13+11i}{10}$$

---

## 2. Polynomials, Division & Root Finding

### **Deriving the Quadratic Formula**

Starting from $ax^2 + bx + c = 0$:

1. Divide by $a$: $x^2 + \frac{b}{a}x = -\frac{c}{a}$

2. Complete the square by adding $\left(\frac{b}{2a}\right)^2$ to both sides:

$$x^2 + \frac{b}{a}x + \left(\frac{b}{2a}\right)^2 = -\frac{c}{a} + \left(\frac{b}{2a}\right)^2$$

3. Simplify right-hand side:

$$\left(x + \frac{b}{2a}\right)^2 = \frac{b^2 - 4ac}{4a^2}$$

4. Take the square root of both sides:

$$x + \frac{b}{2a} = \pm \frac{\sqrt{b^2 - 4ac}}{2a}$$

5. Solve for $x$:

$$x = \frac{-b \pm \sqrt{b^2 - 4ac}}{2a}$$

> **The Discriminant ($b^2 - 4ac$):**
>
> - If negative $\rightarrow$ yields imaginary solutions (which come in complex conjugate pairs: $a \pm bi$).

### **Rational Roots Test**

To find candidate rational roots for $a_n x^n + ... + a_0 = 0$, test all possible values of:

$$\frac{\text{Factors of constant term } a_0}{\text{Factors of leading coefficient } a_n}$$

- _Example:_ For $2x^3 + 3x^2 - 3x - 2 = 0$, constant factors are $\pm 1, \pm 2$; leading factors are $\pm 1, \pm 2$. Tested rational possibilities are $\{\pm 1, \pm 2, \pm 1/2\}$.

### **Synthetic Division**

Used as a shortcut to test factors and divide high-degree polynomials.

- **Example Task:** Check if $x - 2 = 0 \implies x = 2$ is a factor of $x^4 + x^3 - 11x^2 - 5x + 30 = 0$:

$$\begin{array}{r\|rrrrr}     2 & 1 & 1 & -11 & -5 & 30 \\       &   & 2 &   6 & -10 & -30 \\     \hline       & 1 & 3 &  -5 & -15 & \mathbf{0}     \end{array}$$

Since the remainder is $0$, $(x-2)$ is a factor, reducing the expression to $(x-2)(x^3 + 3x^2 - 5x - 15) = 0$.

- Testing $x = -3$ on the quotient yields quotient $(x^2 - 5)$ with remainder $0$.

- Final factors: $(x-2)(x+3)(x^2 - 5) = 0$.

- Roots: $x = 2, -3, \sqrt{5}, -\sqrt{5}$.

---

## 3. Analytic Geometry & Line Equations

### **Lines, Slopes, and Intercepts**

- **Slope Equation:** $m = \frac{\text{rise}}{\text{run}} = \frac{y_2 - y_1}{x_2 - x_1}$

- **Vertical Lines:** Infinite rise with zero run $\rightarrow$ Slope is **undefined**.

- **Horizontal Lines:** Zero rise with infinite run $\rightarrow$ Slope = $0$.

- **$y$-intercept:** Value of $y$ when $x = 0$.

- **Slope-Intercept Form:** $y = mx + c$

- **Parallel Lines:** Have equal slopes ($m_1 = m_2$).

- **Perpendicular Lines:** Slopes are opposite reciprocals ($m_2 = -\frac{1}{m_1}$).

### **Absolute Value Equations**

- $\vert{}x\vert{} = \pm x$.

- _Example:_ $\vert{}x + 3\vert{} = 8 \implies x + 3 = 8$ or $x + 3 = -8 \implies x = 5$ or $x = -11$.

### **Distance Formula**

Distance between points $A(x_1, y_1)$ and $B(x_2, y_2)$:

$$d = \sqrt{(x_2 - x_1)^2 + (y_2 - y_1)^2}$$

---

## 4. Geometry (2D, 3D, & Triangle Centers)

### **Dimensional Hierarchy & Basics**

- **Point:** 0-Dimensional.

- **Line:** 1-Dimensional (extends infinitely in both directions).

- **Ray:** 1 endpoint, extends infinitely in one direction.

- **Line Segment:** Finite, 2 endpoints.

- **Plane:** 2-Dimensional.

- **Space:** 3-Dimensional.

### **Angles & Parallel Transversals**

- **Types:** Acute ($< 90^\circ$), Right ($= 90^\circ$), Obtuse ($> 90^\circ$ and $< 180^\circ$).

- **Complementary:** Add up to $90^\circ$.

- **Supplementary:** Add up to $180^\circ$.

- **Vertical Angles:** Opposite angles formed by intersecting lines (equal).

- **Parallel Line Transversal Properties:**
- Alternate interior angles are equal.

- Alternate exterior angles are equal.

- Corresponding angles are equal.

- Same-side interior angles add up to $180^\circ$.

### **Triangles & Triangle Centers**

- **Sum of interior angles:** Always $180^\circ$.

- **Pythagorean Theorem Proof:** Constructed via a square of side $(a+b)$ containing four right triangles and an inner square of side $c$:

$$(a+b)^2 = 4\left(\frac{1}{2}ab\right) + c^2 \implies a^2 + 2ab + b^2 = 2ab + c^2 \implies a^2 + b^2 = c^2$$

- **Special Right Triangles:**
- $45^\circ - 45^\circ - 90^\circ$: Sides ratio $x : x : x\sqrt{2}$.

- $30^\circ - 60^\circ - 90^\circ$: Sides ratio $x : x\sqrt{3} : 2x$.

- **Triangle Centers:**

| Center Name      | Formed By Intersection Of | Properties |
| ---------------- | ------------------------- | ---------- |
| **Circumcenter** | Perpendicular Bisectors   |

| Equidistant from vertices. Center of circumscribed circle. Inside for acute, outside for obtuse, on hypotenuse for right triangles.

|
| **Incenter** | Angle Bisectors

| Equidistant from all three sides. Center of inscribed circle.

|
| **Centroid** | Medians (vertex to midpoint of opposite side)

| Positioned $\frac{2}{3}$ of the way along the median from vertex to opposite side.

|
| **Orthocenter** | Altitudes (perpendiculars from vertex to opposite side)

| Intersection point of triangle altitudes.

|

- **Triangle Midsegment Theorem:** Midsegment connecting midpoints of two sides is parallel to the third side and half its length.

### **2D Shapes & 3D Solids (Area, Surface Area, Volume)**

| Shape / Solid         | Area / Surface Area Formula               | Volume Formula                               |
| --------------------- | ----------------------------------------- | -------------------------------------------- |
| **Square**            | $A = \text{side}^2$<br>                   | $V = \text{side}^3$<br>                      |
| **Rectangle / Prism** | $A = l \times w$<br>                      | $V = l \cdot w \cdot h$<br>                  |
| **Triangle / Prism**  | $A = \frac{1}{2} b h$<br>                 | $V = \text{Area}_{\text{base}} \times h$<br> |
| **Trapezoid**         | $A = \frac{b_1 + b_2}{2} \cdot h$<br>     | —                                            |
| **Circle / Cylinder** | $A = \pi r^2$, Circumference $C = 2\pi r$ |

| $SA = 2\pi r^2 + 2\pi r h$; $V = \pi r^2 h$

|
| **Pyramid** | $SA = \text{Base Area} + \text{Sides Area}$<br> | $V = \frac{1}{3} \cdot \text{Base Area} \cdot h$<br> |
| **Cone** | — | $V = \frac{1}{3} \pi r^2 h$<br> |
| **Sphere** | $SA = 4\pi r^2$<br> | $V = \frac{4}{3} \pi r^3$<br> |

> **Note on Polyhedra:** A 3D object with flat polygon faces. Cylinders, cones, and spheres are **not** polyhedra because they have curved surfaces.

---

## 5. Functions, Transformations & Graph Analysis

### **Function Definition & Tests**

- **Function Rule:** Must yield only **one output** ($y$) for every given input ($x$).

- **Vertical Line Test:** A graph represents a function if and only if no vertical line intersects it more than once.

- **Horizontal Line Test:** Used to determine if a function is **one-to-one** and has a valid inverse function.

### **Domain, Range, and Multiplicity**

- **Domain:** Set of all valid input values ($x$).

- **Range:** Set of all possible output values ($y$).

- **Zeros & Multiplicity:**
- **Odd Multiplicity:** Graph **crosses** the $x$-axis at the zero.

- **Even Multiplicity:** Graph **touches** (bounces off) the $x$-axis at the zero.

### **General Function Transformations**

For $f(x) = a \cdot g(b(x - h)) + k$:

- $k$: Vertical shift ($+k$ up, $-k$ down).

- $h$: Horizontal shift ($+h$ right, $-h$ left).

- $a$: Vertical stretch ($\vert{}a\vert{} > 1$), compression ($\vert{}a\vert{} < 1$), or reflection across $x$-axis (if $-a$).

- $b$: Horizontal compression ($\vert{}b\vert{} > 1$), stretch ($\vert{}b\vert{} < 1$), or reflection across $y$-axis (if $-b$).

### **End Behavior (Leading Coefficient Test)**

For polynomial $y = a_n x^n$:

1. **Odd degree, positive coeff:** Drops left, rises right.

2. **Odd degree, negative coeff:** Rises left, drops right.

3. **Even degree, positive coeff:** Rises both left and right.

4. **Even degree, negative coeff:** Drops both left and right.

### **Asymptotes of Rational Functions $\frac{f(x)}{g(x)} = \frac{a x^m + ...}{b x^n + ...}$**

- **Vertical Asymptotes (VA):** Values of $x$ making denominator $g(x) = 0$.

- **Horizontal Asymptotes (HA):**
- If $m < n$: $y = 0$.

- If $m = n$: $y = \frac{a}{b}$.

- If $m > n$: No horizontal asymptote (may have slant asymptote).

### **Inverse Functions**

1. Replace $f(x)$ with $y$.

2. Swap $x$ and $y$.

3. Solve for $y$ to get $f^{-1}(x)$.

- **Property:** $f(f^{-1}(x)) = x$ and $f^{-1}(f(x)) = x$.

- **Graphical Rule:** $f^{-1}(x)$ is a reflection of $f(x)$ across the line $y = x$.

- $\text{Domain of } f(x) = \text{Range of } f^{-1}(x)$ and $\text{Range of } f(x) = \text{Domain of } f^{-1}(x)$.

---

## 6. Conic Sections

Conic sections are curves formed by intersecting a cone with a plane.

| Conic Section | Eccentricity ($e$) | Standard Equation (Center at $(h,k)$) | Key Properties / Formulas |
| ------------- | ------------------ | ------------------------------------- | ------------------------- |
| **Circle**    | $e = 0$<br>        | $(x - h)^2 + (y - k)^2 = r^2$<br>     | $r =$ radius.             |

|
| **Ellipse** | $0 < e < 1$<br> | $\frac{(x - h)^2}{a^2} + \frac{(y - k)^2}{b^2} = 1$<br> | Major axis $= 2a$, Minor axis $= 2b$.

<br>

<br>Foci relation: $c^2 = a^2 - b^2$.

|
| **Parabola** | $e = 1$<br> | $y = a(x - h)^2 + k$<br> | Distance to focus = distance to directrix.

<br>

<br>Vertex $(h, k)$ where $h = -\frac{b}{2a}$.

|
| **Hyperbola** | $e > 1$<br> | $\frac{(x - h)^2}{a^2} - \frac{(y - k)^2}{b^2} = 1$<br> | Difference of distances to foci is constant.

<br>

<br>Foci relation: $c^2 = a^2 + b^2$.

<br>

<br>Asymptotes: $y = \pm \frac{b}{a} x$.

|

---

## 7. Exponentials & Logarithms

### **Fundamental Rules & Natural Base $e$**

- **Exponential Form:** $y = b^x$ ($b > 0, b \neq 1$).

- **Natural Base $e$:** Derived from continuous compounding:

$$e = \lim_{n \to \infty} \left(1 + \frac{1}{n}\right)^n \approx 2.71828...$$

- **Logarithmic Definition:** $\log_b(x) = y \iff b^y = x$. Natural log $\ln(x) = \log_e(x)$.

### **Logarithm Properties**

1. $\log_b(b) = 1$

2. $\log_b(1) = 0$

3. **Product Rule:** $\log_b(xy) = \log_b(x) + \log_b(y)$

4. **Quotient Rule:** $\log_b\left(\frac{x}{y}\right) = \log_b(x) - \log_b(y)$

5. **Power Rule:** $\log_b(x^a) = a \log_b(x)$

6. **Change of Base Formula:** $\log_b(x) = \frac{\log_a(x)}{\log_a(b)} = \frac{\ln(x)}{\ln(b)}$

---

## 8. Sequences, Series & Counting

### **Sequences & Factorials**

- **Arithmetic Sequence:** Constant difference between terms ($a_n = 2n + 3 \rightarrow 5, 7, 9, 11$).

- **Geometric Sequence:** Constant multiplier between terms ($a_n = 2 \cdot 3^{n-1} \rightarrow 2, 6, 18, 54$).

- **Fibonacci Sequence:** Recursive formula $a_n = a_{n-1} + a_{n-2}$ ($1, 1, 2, 3, 5, 8, 13...$).

- **Factorial:** $n! = n \times (n-1) \times (n-2) \times ... \times 1$.

- **Euler's Series:** $e = 1 + 1 + \frac{1}{2!} + \frac{1}{3!} + \frac{1}{4!} + ... \approx 2.71828...$

### **Permutations vs. Combinations**

- **Permutations (Order Matters):**

$$P(n, r) = \frac{n!}{(n-r)!}$$

_(If repetitions allowed: $n^r$ possibilities)_.

- **Combinations (Order Does NOT Matter):**

$$C(n, r) = \binom{n}{r} = \frac{n!}{r!(n-r)!}$$

---

## 9. Probability Theory

### **Key Formulas & Rules**

1. **Basic Probability:** $P(A) = \frac{\text{Desired Outcomes}}{\text{Total Outcomes}}$

2. **Complement Rule:** $P(A') = 1 - P(A)$

3. **Addition Rule:** $P(A \cup B) = P(A) + P(B) - P(A \cap B)$

_(If mutually exclusive, $P(A \cap B) = 0$)_.

4. **Multiplication Rule:** $P(A \cap B) = P(A) \cdot P(B\vert{}A)$

_(If independent, $P(A \cap B) = P(A) \cdot P(B)$)_.

5. **Conditional Probability:** $P(B\vert{}A) = \frac{P(A \cap B)}{P(A)}$

6. **Law of Total Probability:** $P(A) = \sum P(A \vert{} B_i) P(B_i)$

7. **Bayes' Theorem:**

$$P(A\vert{}B) = \frac{P(B\vert{}A) \cdot P(A)}{P(B)}$$

---

## 10. Trigonometry

### **Radian Measure & Basic Ratios**

- **Radian Definition:** Angle subtended when arc length equals radius ($1 \text{ rev} = 360^\circ = 2\pi \text{ rad}$).

- **Conversion:** $\text{Radians} = \text{Degrees} \times \frac{\pi}{180^\circ}$.

- **Right Triangle Ratios (SOH CAH TOA):**

$$\sin\theta = \frac{\text{opp}}{\text{hyp}}, \quad \cos\theta = \frac{\text{adj}}{\text{hyp}}, \quad \tan\theta = \frac{\text{opp}}{\text{adj}}$$

$$\csc\theta = \frac{1}{\sin\theta}, \quad \sec\theta = \frac{1}{\cos\theta}, \quad \cot\theta = \frac{1}{\tan\theta}$$

### **Unit Circle Coordinates**

On a unit circle ($r=1$), any point is $(x, y) = (\cos\theta, \sin\theta)$.

### **Core Trigonometric Identities**

- **Pythagorean Identities:**

$$\sin^2\theta + \cos^2\theta = 1$$

$$1 + \tan^2\theta = \sec^2\theta$$

$$1 + \cot^2\theta = \csc^2\theta$$

- **Cofunction Identities:** $\sin(90^\circ - \theta) = \cos\theta$, $\tan(90^\circ - \theta) = \cot\theta$.

- **Sum & Difference Formulas:**

$$\sin(A \pm B) = \sin A \cos B \pm \cos A \sin B$$

$$\cos(A \pm B) = \cos A \cos B \mp \sin A \sin B$$

- **Double Angle Formulas:**

$$\sin(2\theta) = 2\sin\theta\cos\theta$$

$$\cos(2\theta) = \cos^2\theta - \sin^2\theta = 2\cos^2\theta - 1 = 1 - 2\sin^2\theta$$

- **Power Reduction Formulas:**

$$\sin^2\theta = \frac{1 - \cos(2\theta)}{2}, \quad \cos^2\theta = \frac{1 + \cos(2\theta)}{2}$$

### **Laws for Oblique Triangles**

- **Law of Sines:** $\frac{a}{\sin A} = \frac{b}{\sin B} = \frac{c}{\sin C}$

- **Law of Cosines:** $a^2 = b^2 + c^2 - 2bc\cos A$

- **Area of Triangle:** $\text{Area} = \frac{1}{2}bc\sin A$

---

## 11. Polar Coordinates, Parametric Equations & Intro Calculus

### **Polar Coordinates**

Described as $(r, \theta)$ instead of rectangular $(x, y)$.

- **Conversion to Rectangular:** $x = r\cos\theta$, $y = r\sin\theta$.

- **Conversion to Polar:** $r^2 = x^2 + y^2$, $\tan\theta = \frac{y}{x}$.

### **Parametric Equations**

$x$ and $y$ are expressed independently in terms of a third parameter $t$.

- _Example:_ $x = 2t$, $y = t^2$.

- **Eliminating parameter $t$:** $t = \frac{x}{2} \implies y = \left(\frac{x}{2}\right)^2 = \frac{x^2}{4}$ (Equation of a parabola).

### **Introduction to Calculus Concepts**

- **Differential Calculus:** Concerned with instantaneous rate of change and slopes of curves.

- **Integral Calculus:** Concerned with accumulation and area under a curve.

- **Zeno's Paradox:** Conceptual foundation that performing an operation infinitely many times can yield a finite, deterministic result.
